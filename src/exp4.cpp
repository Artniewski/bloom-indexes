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
    int numRecords;
    int bloomTreeRatio;
    int numberOfAttempts;
    size_t itemsPerPartition;
    size_t bloomSize;
    int numHashFunctions;
};

extern void clearBloomFilterFiles(const std::string& dbDir);
extern std::atomic<size_t> gBloomCheckCount;
extern boost::asio::thread_pool globalThreadPool;

void runExp4(std::string baseDir, bool initMode) {
    const std::vector<std::string> columns = {"phone", "mail", "address"};
    const std::vector<int> dbSizes = {1'000'000, 4'000'000};

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& dbSize : dbSizes) {
        TestParams params = {baseDir + "/exp4_db_" + std::to_string(dbSize), dbSize, 3, 1, 100000, 1'000'000, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);
        
        clearBloomFilterFiles(params.dbName);
        dbManager.openDB(params.dbName);

        if (!initMode) {
            dbManager.insertRecords(params.numRecords, columns);
            spdlog::info("ExpBloomMetrics: 10 second sleep...");
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }

        std::map<std::string, BloomTree> hierarchies;
        std::mutex hierarchiesMutex;
        
        // First asynchronously get all SST files for all columns
        std::map<std::string, std::vector<std::string>> columnSstFiles;
        std::vector<std::future<std::pair<std::string, std::vector<std::string>>>> scanFutures;
        
        for (const auto& column : columns) {
            std::promise<std::pair<std::string, std::vector<std::string>>> scanPromise;
            scanFutures.push_back(scanPromise.get_future());
            
            boost::asio::post(globalThreadPool, [column, &dbManager, &params, promise = std::move(scanPromise)]() mutable {
                auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, column);
                promise.set_value(std::make_pair(column, std::move(sstFiles)));
            });
        }
        
        // Wait for all scanning to complete
        for (auto& fut : scanFutures) {
            auto [column, sstFiles] = fut.get();
            columnSstFiles[column] = std::move(sstFiles);
        }
        
        // Now process each column's hierarchy building sequentially
        // (createPartitionedHierarchy already has internal parallelism)
        for (const auto& [column, sstFiles] : columnSstFiles) {
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.numHashFunctions, params.bloomTreeRatio);
            spdlog::info("Hierarchy built for column: {}", column);
            hierarchies.try_emplace(column, std::move(hierarchy));
        }

        std::ofstream out(baseDir + "/exp_4_bloom_metrics.csv", std::ios::app);
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
        size_t bloomChecks = gBloomCheckCount.load();
        spdlog::info("Global Total bloom‐filter checks this query: {}", bloomChecks);
        // Reset for the next experiment if you like:
        gBloomCheckCount = 0;
        // --- Hierarchical Multi-Column Query ---
        stopwatch.start();
        std::vector<std::string> hierarchicalMatches = multiColumnQueryHierarchical(queryTrees, expectedValues, "", "", dbManager);
        stopwatch.stop();
        auto hierarchicalMultiTime = stopwatch.elapsedMicros();
        bloomChecks = gBloomCheckCount.load();
        spdlog::info("Multi Total bloom‐filter checks this query: {}", bloomChecks);

        // Reset for the next experiment if you like:
        gBloomCheckCount = 0;
        // --- Hierarchical Single Column Query ---
        stopwatch.start();
        std::vector<std::string> singlehierarchyMatches = dbManager.findUsingSingleHierarchy(queryTrees[0], columns, expectedValues);
        stopwatch.stop();
        auto hierarchicalSingleTime = stopwatch.elapsedMicros();
        bloomChecks = gBloomCheckCount.load();
        spdlog::info("Single Total bloom‐filter checks this query: {}", bloomChecks);

        // Reset for the next experiment if you like:
        gBloomCheckCount = 0;
        // Zapis wyników do pliku CSV
        out << params.numRecords << ","
            << dbSize << ","
            << globalScanTime << ","
            << hierarchicalSingleTime << ","
            << hierarchicalMultiTime << "\n";
        out.close();
        dbManager.closeDB();
    }
} 