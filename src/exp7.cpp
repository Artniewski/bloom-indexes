#include <spdlog/spdlog.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "bloomTree.hpp"
#include "bloom_manager.hpp"
#include "db_manager.hpp"
#include "stopwatch.hpp"

// Forward declaration for function from algorithm.hpp
std::vector<std::string> multiColumnQueryHierarchical(std::vector<BloomTree>& trees,
                                                     const std::vector<std::string>& values,
                                                     const std::string& globalStart,
                                                     const std::string& globalEnd,
                                                     DBManager& dbManager);

// Import necessary declarations from main.cpp
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

// Kolumny: Global Scan| Hierarchical Single Column | Hierarchical Multi-Column
// Wiersze: ilość itemów spełniających kryteria: 2, 4, 6, 8, 10
void runExp7(std::string baseDir, bool initMode) {
    const int dbSize = 1'000'000;
    const std::vector<std::string> columns = {"phone", "mail", "address"};
    const std::vector<int> targetItems = { 2, 4, 6, 8, 10};
    
    std::vector<int> randomIndices;
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist(1, dbSize);
        
        std::unordered_set<int> uniqueIndices;
        while (uniqueIndices.size() < 10) {
            uniqueIndices.insert(dist(gen));
        }
        
        randomIndices.assign(uniqueIndices.begin(), uniqueIndices.end());
        std::sort(randomIndices.begin(), randomIndices.end());
        
        std::string indicesStr;
        for (int idx : randomIndices) {
            indicesStr += std::to_string(idx) + ", ";
        }
        spdlog::info("Generated 10 random indices: {}", indicesStr);
    }

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& numItems : targetItems) {
        TestParams params = {baseDir + "/exp7_db_" + std::to_string(numItems), false, dbSize, 3, 1, 100000, 1'000'000, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);

        clearBloomFilterFiles(params.dbName);
        dbManager.openDB(params.dbName, params.compactionLogging);

        if (!initMode) {
            // Create a subset of random indices based on numItems
            std::unordered_set<int> targetIndices;
            for (size_t i = 0; i < numItems && i < randomIndices.size(); i++) {
                targetIndices.insert(randomIndices[i]);
            }
            
            // Log which indices we're using
            std::string indicesStr;
            for (int idx : targetIndices) {
                indicesStr += std::to_string(idx) + ", ";
            }
            spdlog::info("Using {} target indices: {}", targetIndices.size(), indicesStr);
            
            // Use the modified DB manager method to insert records with specific target indices
            dbManager.insertRecordsWithSearchTargets(params.numRecords, columns, targetIndices);
            
            spdlog::info("ExpBloomMetrics: 10 second sleep...");
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }

        std::map<std::string, BloomTree> hierarchies;
        std::vector<std::future<std::pair<std::string, BloomTree>>> futures;

        for (const auto& column : columns) {
            futures.push_back(std::async(std::launch::async, [&dbManager, &bloomManager, &params](const std::string& col) -> std::pair<std::string, BloomTree> {
            auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, col);
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.numHashFunctions, params.bloomTreeRatio);
            spdlog::info("Hierarchy built for column: {}", col);
            return { col, std::move(hierarchy) }; }, column));
        }

        for (auto& fut : futures) {
            auto [col, tree] = fut.get();
            hierarchies.try_emplace(col, std::move(tree));
        }

        std::ofstream out(baseDir + "/exp_7_bloom_metrics.csv", std::ios::app);
        if (!out) {
            spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
            return;
        }

        // hierarchies vector
        std::vector<BloomTree> queryTrees;
        std::vector<std::string> expectedValues;

        spdlog::info("value in half of the db: {}", columns[0] + "_value" + std::to_string(dbSize / 2));

        for (const auto& column : columns) {
            queryTrees.push_back(hierarchies.at(column));
            const std::string expectedValue = column + "_target";
            spdlog::info("ExpBloomMetrics: expectedValue: {}", expectedValue);
            expectedValues.push_back(expectedValue);
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
            << numItems << ","
            << globalScanTime << ","
            << hierarchicalSingleTime << ","
            << hierarchicalMultiTime << "\n";
    }
} 