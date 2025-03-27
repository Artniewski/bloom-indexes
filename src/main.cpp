#include <spdlog/spdlog.h>

#include <chrono>
#include <future>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "algorithm.hpp"
#include "bloomTree.hpp"
#include "bloom_manager.hpp"
#include "db_manager.hpp"
#include "stopwatch.hpp"

// Test parameter structure.
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

void runColumnTest(int attemptIndex,
                   const TestParams& params,
                   const std::vector<std::string>& allColumns,
                   const std::vector<std::string>& selectedColumns,
                   const std::vector<std::string>& values) {
    std::string dbName = params.dbName + "_" + std::to_string(attemptIndex);
    spdlog::info("Running test on DB: {}", dbName);

    DBManager dbManager;
    BloomManager bloomManager;

    // Open DB and insert records across all columns.
    dbManager.openDB(dbName, params.compactionLogging);
    dbManager.insertRecords(params.numRecords, allColumns);

    std::this_thread::sleep_for(std::chrono::seconds(5));

    // Build a BloomTree hierarchy for each column.
    std::map<std::string, BloomTree> hierarchies;
    std::vector<std::future<std::pair<std::string, BloomTree>>> futures;

    for (const auto& column : allColumns) {
        futures.push_back(std::async(std::launch::async, [&dbManager, &bloomManager, dbName, &params](const std::string& col) -> std::pair<std::string, BloomTree> {
            auto sstFiles = dbManager.scanSSTFilesForColumn(dbName, col);
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.bloomTreeRatio, params.numHashFunctions);
            spdlog::info("Hierarchy built for column: {}", col);
            return { col, std::move(hierarchy) }; }, column));
    }

    // Collect results and build the map.
    for (auto& fut : futures) {
        auto [col, tree] = fut.get();
        hierarchies.try_emplace(col, std::move(tree));
    }

    // --- Global Scan Query ---
    // Use the selected columns and expected values to scan the entire DB.
    spdlog::critical("### Global Scan Query ###");
    auto globalMatches = dbManager.scanForRecordsInColumns(selectedColumns, values);
    for (const auto& key : globalMatches) {
        spdlog::debug("[Global] Match key: {}...", key.substr(0, 30));
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // --- Hierarchical Multi-Column Query ---
    // Build vectors of BloomTrees and query values for the selected columns.
    std::vector<BloomTree> queryTrees;
    std::vector<std::string> queryValues;
    for (const auto& col : selectedColumns) {
        if (hierarchies.find(col) != hierarchies.end()) {
            queryTrees.push_back(hierarchies.at(col));
        } else {
            spdlog::warn("Hierarchy for column {} not found.", col);
        }
    }
    queryValues = values;  // Order must match the selectedColumns vector.

    spdlog::info("### Hierarchical Multi-Column Query ###");
    auto hierarchicalMatches = multiColumnQueryHierarchical(queryTrees, queryValues, "", "", dbManager);

    for (const auto& key : hierarchicalMatches) {
        spdlog::debug("[Multi] Match key: {}...", key.substr(0, 30));
    }

    // --- Hierarch Single Column Query ---
    // Use the first column to query the hierarchy and then scan the DB for the remaining columns.
    spdlog::info("### Hierarchical Single Column Query ###");
    auto singlehierarchyMatches = dbManager.findUsingSingleHierarchy(hierarchies.at(selectedColumns[0]), selectedColumns, values);

    for (const auto& key : singlehierarchyMatches) {
        spdlog::debug("[Single] Match key: {}...", key.substr(0, 30));
    }

    dbManager.closeDB();
    spdlog::info("Test for DB '{}' completed.\n", dbName);
}

int main() {
    try {
        auto timeNowInEpoch = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        spdlog::set_level(spdlog::level::debug);

        std::vector<std::string> allColumns = {"default", "phone", "mail", "address", "name", "surname"};
        std::vector<std::string> selectedColumns = {"phone", "name", "mail"};

        // Define search values for the selected columns.
        // Adjust these values to match a record inserted into your DB.
        std::vector<std::string> values1 = {"phone_value54321" + std::string(1000, 'a'),
                                            "name_value54321" + std::string(1000, 'a'),
                                            "mail_value54321" + std::string(1000, 'a')};
        std::vector<std::string> values2 = {"phone_value350000" + std::string(1000, 'a'),
                                            "name_value350000" + std::string(1000, 'a'),
                                            "mail_value350000" + std::string(1000, 'a')};
        std::vector<std::string> values3 = {"phone_value3500000" + std::string(1000, 'a'),
                                            "name_value3500000" + std::string(1000, 'a'),
                                            "mail_value3500000" + std::string(1000, 'a')};

        // Define three different test parameter sets for varying DB sizes.
        TestParams paramsSmall = {"db_small", false, 600'000, 3, 1, 10000, 100'000, 6};
        TestParams paramsMedium = {"db_medium", false, 1'000'000, 3, 1, 100000, 1'000'000, 6};
        TestParams paramsLarge = {"db_large", false, 4'000'000, 3, 1, 100000, 1'000'000, 6};

        // Run tests for each DB size.
        runColumnTest(1, paramsSmall, allColumns, selectedColumns, values1);
        runColumnTest(2, paramsMedium, allColumns, selectedColumns, values2);
        runColumnTest(3, paramsLarge, allColumns, selectedColumns, values3);
    } catch (const std::exception& e) {
        spdlog::error("[Error] {}", e.what());
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
