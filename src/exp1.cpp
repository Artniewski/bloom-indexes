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

void runExp1(std::string baseDir, bool initMode) {
    // Zapis headerow
    std::ofstream out(baseDir + "/exp_1_bloom_metrics.csv", std::ios::app);
    if (!out) {
        spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
        return;
    }
    out << "numRecords,bloomTreeRatio,itemsPerPartition,bloomSize,numHashFunctions,singleHierarchyLeafs,bloomDiskSize,blomMemSize" << "\n";
    out.close();

    const std::vector<std::string> columns = {"phone", "mail", "address"};
    const std::vector<int> dbSizes = {10'000'000, 2'000'000, 3'000'000};

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& dbSize : dbSizes) {
        TestParams params = {baseDir + "/exp1_db_" + std::to_string(dbSize), true, dbSize, 3, 1, 100000, 1'000'000, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);
        clearBloomFilterFiles(params.dbName);
        dbManager.openDB(params.dbName, params.compactionLogging, columns);

        if (!initMode) {
            dbManager.insertRecords(params.numRecords, columns);
            try {
                dbManager.compactAllColumnFamilies();
            } catch (const std::exception& e) {
                spdlog::error("Error '{}'", e.what());
                exit(1);
            }
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

        size_t totalDiskBloomSize = 0;
        size_t totalMemoryBloomSize = 0;
        for (const auto& kv : hierarchies) {
            const BloomTree& tree = kv.second;
            totalDiskBloomSize += tree.diskSize();
            totalMemoryBloomSize += tree.memorySize();
        }

        auto allDbSize = 0;

        // Zapis wyników do pliku CSV
        std::ofstream out(baseDir + "/exp_1_bloom_metrics.csv", std::ios::app);
        if (!out) {
            spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
            return;
        }
        out << params.numRecords << ","
            << params.bloomTreeRatio << ","
            << params.itemsPerPartition << ","
            << params.bloomSize << ","
            << params.numHashFunctions << ","
            << hierarchies.at(columns[0]).leafNodes.size() << ","
            << totalDiskBloomSize << ","
            << totalMemoryBloomSize << "\n";
        out.close();
        dbManager.closeDB();
        spdlog::info("ExpBloomMetrics: Eksperyment dla bazy '{}' zakończony.", params.dbName);
    }
} 