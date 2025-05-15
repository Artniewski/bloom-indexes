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

void runExp8(std::string baseDir, bool initMode) {
    const int dbSize = 1'000'000;
    const std::vector<int> numColumns = {2, 4, 8, 10};

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& numCol : numColumns) {
        std::vector<std::string> columns;
        for (int i = 0; i < numCol; ++i) {
            columns.push_back("i_" + std::to_string(i) + "_column");
        }
        // log columns
        for (const auto& column : columns) {
            spdlog::info("Column: {}", column);
        }

        TestParams params = {baseDir + "/exp8_db_" + std::to_string(numCol), false, dbSize, 3, 1, 100000, 1'000'000, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);
        clearBloomFilterFiles(params.dbName);
        dbManager.openDB(params.dbName, params.compactionLogging, columns);

        if (!initMode) {
            dbManager.insertRecords(params.numRecords, columns);
            spdlog::info("ExpBloomMetrics: 10 second sleep...");
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }

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

        std::ofstream out(baseDir + "/exp_8_bloom_metrics.csv", std::ios::app);
        if (!out) {
            spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
            return;
        }

        // hierarchies vector
        std::vector<BloomTree> queryTrees;
        std::vector<std::string> expectedValues;
        std::string expectedValueSuffix = "_value" + std::to_string(dbSize / 2);
        for (const auto& column : columns) {
            queryTrees.push_back(hierarchies.at(column));
            expectedValues.push_back(column + expectedValueSuffix);
        }

        // --- Global Scan Query ---
        StopWatch stopwatch;
        stopwatch.start();
        std::vector<std::string> globalMatches = dbManager.scanForRecordsInColumns(columns, expectedValues);
        stopwatch.stop();
        auto globalScanTime = stopwatch.elapsedMicros();
        // --- Hierarchical Multi-Column Query ---
        stopwatch.start();
        std::vector<std::string> hierarchicalMatches = multiColumnQueryHierarchical(queryTrees, expectedValues, "", "", dbManager);
        stopwatch.stop();
        auto hierarchicalMultiTime = stopwatch.elapsedMicros();
        // --- Hierarchical Single Column Query ---
        stopwatch.start();
        std::vector<std::string> singlehierarchyMatches = dbManager.findUsingSingleHierarchy(queryTrees[0], columns, expectedValues);
        stopwatch.stop();
        auto hierarchicalSingleTime = stopwatch.elapsedMicros();
        // Zapis wyników do pliku CSV
        out << params.numRecords << ","
            << numCol << ","
            << globalScanTime << ","
            << hierarchicalSingleTime << ","
            << hierarchicalMultiTime << "\n";
        out.close();
        dbManager.closeDB();
    }
} 