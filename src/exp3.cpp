#include <spdlog/spdlog.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>

#include "bloomTree.hpp"
#include "bloom_manager.hpp"
#include "db_manager.hpp"
#include "stopwatch.hpp"
#include "algorithm.hpp"



struct TestParams {
    std::string dbName;
    bool compactionLogging;
    int numRecords;
    int bloomTreeRatio;
    int numberOfAttempts;
    size_t itemsPerPartition;
    size_t bloomSize;
    int numHashFunctions;
};

extern void clearBloomFilterFiles(const std::string& dbDir);
extern boost::asio::thread_pool globalThreadPool;

void runExp3(std::string baseDir, bool initMode) {
    const std::vector<std::string> columns = {"phone", "mail", "address"};
    const std::vector<int> dbSizes = {1'000'000, 4'000'000};

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& dbSize : dbSizes) {
        TestParams params = {baseDir + "/exp3_db_" + std::to_string(dbSize), false, dbSize, 3, 1, 100000, 1'000'000, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);

        // dbManager.openDB(params.dbName, params.compactionLogging);

        // if (!initMode) {
        //     dbManager.insertRecords(params.numRecords, columns);
        // }

        StopWatch stopwatch;
        stopwatch.start();
        dbManager.openDB(params.dbName, params.compactionLogging);
        dbManager.insertRecords(params.numRecords, columns);
        stopwatch.stop();
        auto dbCreationTime = stopwatch.elapsedMicros();

        spdlog::info("ExpBloomMetrics: 10 second sleep...");
        std::this_thread::sleep_for(std::chrono::seconds(10));

        stopwatch.start();
        std::map<std::string, BloomTree> hierarchies;
        std::mutex hierarchiesMutex;
        std::vector<std::future<void>> futures;
        std::atomic<int> tasksRemaining(columns.size());

        for (const auto& column : columns) {
            std::promise<void> taskPromise;
            futures.push_back(taskPromise.get_future());
            
            boost::asio::post(globalThreadPool, [&dbManager, &bloomManager, &params, column, &hierarchies, &hierarchiesMutex, promise = std::move(taskPromise), &tasksRemaining]() mutable {
                auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, column);
                BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                    sstFiles, params.itemsPerPartition, params.bloomSize, params.numHashFunctions, params.bloomTreeRatio);
                spdlog::info("Hierarchy built for column: {}", column);
                
                // Safely store result in the map
                {
                    std::lock_guard<std::mutex> lock(hierarchiesMutex);
                    hierarchies.try_emplace(column, std::move(hierarchy));
                }
                
                promise.set_value();
                
                if (--tasksRemaining == 0) {
                    // All tasks completed
                }
            });
        }

        // Wait for all tasks to complete
        for (auto& fut : futures) {
            fut.wait();
        }
        
        stopwatch.stop();
        auto bloomCreationTime = stopwatch.elapsedMicros();

        // Zapis wyników do pliku CSV
        std::ofstream out(baseDir + "/exp_3_bloom_metrics.csv", std::ios::app);
        if (!out) {
            spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
            return;
        }
        // Format CSV: numRecords, dbSize, bloomCreationTime, dbCreationTime
        out << params.numRecords << ","
            << dbSize << ","
            << bloomCreationTime << ","
            << dbCreationTime << "\n";
        out.close();
        dbManager.closeDB();
    }
} 