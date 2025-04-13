#include <spdlog/spdlog.h>

#include <chrono>
#include <filesystem>
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

    spdlog::info("ExpBloomMetrics: 10 second sleep...");
    std::this_thread::sleep_for(std::chrono::seconds(10));

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

// ############# EXP1 ####################

void runExp1(std::string baseDir) {
    const std::vector<std::string> columns = {"phone", "mail", "address"};
    const std::vector<int> dbSizes = {1'000'000, 2'000'000, 3'000'000};

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& dbSize : dbSizes) {
        TestParams params = {baseDir + "/exp1_db_" + std::to_string(dbSize), false, dbSize, 3, 1, 100000, 1'000'000, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);

        dbManager.openDB(params.dbName, params.compactionLogging);
        dbManager.insertRecords(params.numRecords, columns);

        spdlog::info("ExpBloomMetrics: 10 second sleep...");
        std::this_thread::sleep_for(std::chrono::seconds(10));

        std::map<std::string, BloomTree> hierarchies;
        std::vector<std::future<std::pair<std::string, BloomTree>>> futures;

        for (const auto& column : columns) {
            futures.push_back(std::async(std::launch::async, [&dbManager, &bloomManager, &params](const std::string& col) -> std::pair<std::string, BloomTree> {
            auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, col);
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.bloomTreeRatio, params.numHashFunctions);
            spdlog::info("Hierarchy built for column: {}", col);
            return { col, std::move(hierarchy) }; }, column));
        }

        for (auto& fut : futures) {
            auto [col, tree] = fut.get();
            hierarchies.try_emplace(col, std::move(tree));
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

        // Format CSV: numRecords, dbSize, diskBloomSize, memoryBloomSize
        out << params.numRecords << ","
            << allDbSize << ","
            << totalDiskBloomSize << ","
            << totalMemoryBloomSize << "\n";
        out.close();
        dbManager.closeDB();
        spdlog::info("ExpBloomMetrics: Eksperyment dla bazy '{}' zakończony.", params.dbName);
    }
}

// ############# EXP2 ####################
// same for: Exp2. Cel: Ile miejsca zajmują filtry Blooma Założenia:
// columns=3,bloomTreeRatio=3, numRecords=50M,
// Kolumny:Rozmiar bazy danych| rozmiar filtrów Blooma na dysku | rozmiar filtrów Blooma w pamięci RAM
//  Wiersze: dla itemsPerPartition: 50000, 100000, 200000
void runExp2(std::string baseDir) {
    const std::vector<std::string> columns = {"phone", "mail", "address"};
    int dbSize = 1'000'000;
    const std::vector<size_t> itemsPerPartition = {50000};

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& items : itemsPerPartition) {
        TestParams params = {baseDir + "/exp2_db_" + std::to_string(items), false, dbSize, 3, 1, items, 1000000, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);

        dbManager.openDB(params.dbName, params.compactionLogging);
        dbManager.insertRecords(params.numRecords, columns);

        spdlog::info("ExpBloomMetrics: 10 second sleep...");
        std::this_thread::sleep_for(std::chrono::seconds(10));

        std::map<std::string, BloomTree> hierarchies;
        std::vector<std::future<std::pair<std::string, BloomTree>>> futures;

        for (const auto& column : columns) {
            futures.push_back(std::async(std::launch::async, [&dbManager, &bloomManager, &params](const std::string& col) -> std::pair<std::string, BloomTree> {
            auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, col);
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.bloomTreeRatio, params.numHashFunctions);
            spdlog::info("Hierarchy built for column: {}", col);
            return { col, std::move(hierarchy) }; }, column));
        }

        for (auto& fut : futures) {
            auto [col, tree] = fut.get();
            hierarchies.try_emplace(col, std::move(tree));
        }

        size_t totalDiskBloomSize = 0;
        size_t totalMemoryBloomSize = 0;
        for (const auto& kv : hierarchies) {
            const BloomTree& tree = kv.second;
            totalDiskBloomSize += tree.diskSize();
            totalMemoryBloomSize += tree.memorySize();
        }

        // Zapis wyników do pliku CSV
        std::ofstream out(baseDir + "/exp_2_bloom_metrics.csv", std::ios::app);
        if (!out) {
            spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
            return;
        }

        // Format CSV: numRecords,itemsPerPartition, dbSize, diskBloomSize, memoryBloomSize
        out << params.numRecords << ","
            << items << ","
            << dbSize << ","
            << totalDiskBloomSize << ","
            << totalMemoryBloomSize << "\n";
        out.close();
        dbManager.closeDB();
    }
}
// ############# EXP3 ####################
// Exp3.
// Cel: Ile czasu zajmuje tworzenie filtrów Blooma w stosunku do całej bazy
// Założenia: columns=3,bloomTreeRatio=3, itemsPerPartition= 100000
// Kolumny: Czas tworzenia bazy danych| Czas tworzenia fitrów Blooma na dysku | Czas tworzenia fitrów Blooma na pamięci RAM
// Wiersze: dla numRecords: 10M, 50M, 100M, 500M
void runExp3(std::string baseDir) {
    const std::vector<std::string> columns = {"phone", "mail", "address"};
    const std::vector<int> dbSizes = {1'000'000, 4'000'000};

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& dbSize : dbSizes) {
        TestParams params = {baseDir + "/exp3_db_" + std::to_string(dbSize), false, dbSize, 3, 1, 100000, 1'000'000, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);

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
        std::vector<std::future<std::pair<std::string, BloomTree>>> futures;

        for (const auto& column : columns) {
            futures.push_back(std::async(std::launch::async, [&dbManager, &bloomManager, &params](const std::string& col) -> std::pair<std::string, BloomTree> {
            auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, col);
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.bloomTreeRatio, params.numHashFunctions);
            spdlog::info("Hierarchy built for column: {}", col);
            return { col, std::move(hierarchy) }; }, column));
        }

        for (auto& fut : futures) {
            auto [col, tree] = fut.get();
            hierarchies.try_emplace(col, std::move(tree));
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

// ############# EXP4 ####################
// Cel: Porównanie metod przeszukiwania (jedna szukana wartość istniejąca w bazie)
// Założenia: columns=3,bloomTreeRatio=3, itemsPerPartition= 100000
// Kolumny: Global Scan| Hierarchical Single Column | Hierarchical Multi-Column
// Wiersze: dla numRecords: 10M, 50M, 100M, 500M
void runExp4(std::string baseDir) {
    const std::vector<std::string> columns = {"phone", "mail", "address"};
    const std::vector<int> dbSizes = {1'000'000, 4'000'000};

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& dbSize : dbSizes) {
        TestParams params = {baseDir + "/exp4_db_" + std::to_string(dbSize), false, dbSize, 3, 1, 100000, 1'000'000, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);

        dbManager.openDB(params.dbName, params.compactionLogging);
        dbManager.insertRecords(params.numRecords, columns);

        spdlog::info("ExpBloomMetrics: 10 second sleep...");
        std::this_thread::sleep_for(std::chrono::seconds(10));

        std::map<std::string, BloomTree> hierarchies;
        std::vector<std::future<std::pair<std::string, BloomTree>>> futures;

        for (const auto& column : columns) {
            futures.push_back(std::async(std::launch::async, [&dbManager, &bloomManager, &params](const std::string& col) -> std::pair<std::string, BloomTree> {
            auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, col);
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.bloomTreeRatio, params.numHashFunctions);
            spdlog::info("Hierarchy built for column: {}", col);
            return { col, std::move(hierarchy) }; }, column));
        }

        for (auto& fut : futures) {
            auto [col, tree] = fut.get();
            hierarchies.try_emplace(col, std::move(tree));
        }

        std::ofstream out(baseDir + "/exp_4_bloom_metrics.csv", std::ios::app);
        if (!out) {
            spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
            return;
        }

        // hierarchies vector
        std::vector<BloomTree> queryTrees;
        std::vector<std::string> expectedValues;
        std::string expectedValueSuffix = "_value" + std::to_string(dbSize / 2) + std::string(1000, 'a');
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
            << dbSize << ","
            << globalScanTime << ","
            << hierarchicalSingleTime << ","
            << hierarchicalMultiTime << "\n";
        out.close();
        dbManager.closeDB();
    }
}

// ############# EXP5 ####################
// Cel: Porównanie metod przeszukiwania (jedna szukana wartość istniejąca w bazie)
// Założenia: columns=3,bloomTreeRatio=3, numRecords= 50M
// Kolumny: Global Scan| Hierarchical Single Column | Hierarchical Multi-Column
// Wiersze: dla itemsPerPartition: 50000, 100000, 200000
void runExp5(std::string baseDir) {
    const std::vector<std::string> columns = {"phone", "mail", "address"};
    const int dbSize = 4'000'000;
    const int bloomSize = 1'000'000;
    const std::vector<size_t> itemsPerPartition = {50000, 100000, 200000};

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& partitionSize : itemsPerPartition) {
        TestParams params = {baseDir + "/exp5_db_" + std::to_string(partitionSize), false, dbSize, 3, 1, partitionSize, bloomSize, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);

        dbManager.openDB(params.dbName, params.compactionLogging);
        dbManager.insertRecords(params.numRecords, columns);

        spdlog::info("ExpBloomMetrics: 10 second sleep...");
        std::this_thread::sleep_for(std::chrono::seconds(10));

        std::map<std::string, BloomTree> hierarchies;
        std::vector<std::future<std::pair<std::string, BloomTree>>> futures;

        for (const auto& column : columns) {
            futures.push_back(std::async(std::launch::async, [&dbManager, &bloomManager, &params](const std::string& col) -> std::pair<std::string, BloomTree> {
            auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, col);
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.bloomTreeRatio, params.numHashFunctions);
            spdlog::info("Hierarchy built for column: {}", col);
            return { col, std::move(hierarchy) }; }, column));
        }

        for (auto& fut : futures) {
            auto [col, tree] = fut.get();
            hierarchies.try_emplace(col, std::move(tree));
        }

        std::ofstream out(baseDir + "/exp_5_bloom_metrics.csv", std::ios::app);
        if (!out) {
            spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
            return;
        }

        // hierarchies vector
        std::vector<BloomTree> queryTrees;
        std::vector<std::string> expectedValues;
        std::string expectedValueSuffix = "_value" + std::to_string(dbSize / 2) + std::string(1000, 'a');
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
            << partitionSize << ","
            << globalScanTime << ","
            << hierarchicalSingleTime << ","
            << hierarchicalMultiTime << "\n";
        out.close();
        dbManager.closeDB();
    }
}

// ############ EXP6 ####################
// Cel: Porównanie metod przeszukiwania (jedna szukana wartość istniejąca w bazie)
// Założenia: columns=3,bloomTreeRatio=3, numRecords= 50M, itemsPerPartition= 100000
// Kolumny: Global Scan| Hierarchical Single Column | Hierarchical Multi-Column
// Wiersze: dla bloomSize: 500000, 1000000, 2000000

void runExp6(std::string baseDir) {
    const std::vector<std::string> columns = {"phone", "mail", "address"};
    const int dbSize = 4'000'000;
    const std::vector<size_t> bloomSizes = {500000, 1000000, 2000000};

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& bloomSize : bloomSizes) {
        TestParams params = {baseDir + "/exp6_db_" + std::to_string(bloomSize), false, dbSize, 3, 1, 100000, bloomSize, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);

        dbManager.openDB(params.dbName, params.compactionLogging);
        dbManager.insertRecords(params.numRecords, columns);

        spdlog::info("ExpBloomMetrics: 10 second sleep...");
        std::this_thread::sleep_for(std::chrono::seconds(10));

        std::map<std::string, BloomTree> hierarchies;
        std::vector<std::future<std::pair<std::string, BloomTree>>> futures;

        for (const auto& column : columns) {
            futures.push_back(std::async(std::launch::async, [&dbManager, &bloomManager, &params](const std::string& col) -> std::pair<std::string, BloomTree> {
            auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, col);
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.bloomTreeRatio, params.numHashFunctions);
            spdlog::info("Hierarchy built for column: {}", col);
            return { col, std::move(hierarchy) }; }, column));
        }

        for (auto& fut : futures) {
            auto [col, tree] = fut.get();
            hierarchies.try_emplace(col, std::move(tree));
        }

        std::ofstream out(baseDir + "/exp_6_bloom_metrics.csv", std::ios::app);
        if (!out) {
            spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
            return;
        }

        // hierarchies vector
        std::vector<BloomTree> queryTrees;
        std::vector<std::string> expectedValues;
        std::string expectedValueSuffix = "_value" + std::to_string(dbSize / 2) + std::string(1000, 'a');
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
            << bloomSize << ","
            << globalScanTime << ","
            << hierarchicalSingleTime << ","
            << hierarchicalMultiTime << "\n";
        out.close();
        dbManager.closeDB();
    }
}

// ############# EXP7 ####################
// Cel: Porównanie metod przeszukiwania (ilość szukanych itemow: 10)
// Założenia: columns=3,bloomTreeRatio=3, numRecords= 50M,  itemsPerPartition= 100000
// Kolumny: Global Scan| Hierarchical Single Column | Hierarchical Multi-Column
// Wiersze: ilość itemów spełniających kryteria: 2, 4, 6, 8, 10
void runExp7(std::string baseDir) {
    const int dbSize = 4'000'000;
    const std::vector<std::string> columns = {"phone", "mail", "address"};
    const std::vector<int> targetItems = {2, 4, 6, 8, 10};
    std::string searchPattern = std::string(1000, 'X');

    DBManager dbManager;
    BloomManager bloomManager;

    for (const auto& numItems : targetItems) {
        TestParams params = {baseDir + "/exp7_db_" + std::to_string(numItems), false, dbSize, 3, 1, 100000, 1'000'000, 6};
        spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'", params.dbName);

        dbManager.openDB(params.dbName, params.compactionLogging);
        dbManager.insertRecordsWithSearchTargets(params.numRecords, columns, numItems, searchPattern);

        spdlog::info("ExpBloomMetrics: 10 second sleep...");
        std::this_thread::sleep_for(std::chrono::seconds(10));

        std::map<std::string, BloomTree> hierarchies;
        std::vector<std::future<std::pair<std::string, BloomTree>>> futures;

        for (const auto& column : columns) {
            futures.push_back(std::async(std::launch::async, [&dbManager, &bloomManager, &params](const std::string& col) -> std::pair<std::string, BloomTree> {
            auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, col);
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.bloomTreeRatio, params.numHashFunctions);
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
        std::string expectedValueSuffix = "_value" + std::to_string(dbSize / 2) + std::string(1000, 'a');
        for (const auto& column : columns) {
            queryTrees.push_back(hierarchies.at(column));
            expectedValues.push_back(searchPattern);
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

// ############# EXP8 ####################
// Cel: Porównanie metod przeszukiwania (jedna szukana wartość istniejąca w bazie)
// Założenia: bloomTreeRatio=3, numRecords= 50M,  itemsPerPartition= 100000
// Kolumny: Global Scan| Hierarchical Single Column | Hierarchical Multi-Column
// Wiersze: ilość kolumn: 2,4,8,10
void runExp8(std::string baseDir) {
    const int dbSize = 4'000'000;
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

        dbManager.openDB(params.dbName, params.compactionLogging, columns);
        dbManager.insertRecords(params.numRecords, columns);

        spdlog::info("ExpBloomMetrics: 10 second sleep...");
        std::this_thread::sleep_for(std::chrono::seconds(10));

        std::map<std::string, BloomTree> hierarchies;
        std::vector<std::future<std::pair<std::string, BloomTree>>> futures;

        for (const auto& column : columns) {
            futures.push_back(std::async(std::launch::async, [&dbManager, &bloomManager, &params](const std::string& col) -> std::pair<std::string, BloomTree> {
            auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, col);
            BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
                sstFiles, params.itemsPerPartition, params.bloomSize, params.bloomTreeRatio, params.numHashFunctions);
            spdlog::info("Hierarchy built for column: {}", col);
            return { col, std::move(hierarchy) }; }, column));
        }

        for (auto& fut : futures) {
            auto [col, tree] = fut.get();
            hierarchies.try_emplace(col, std::move(tree));
        }

        std::ofstream out(baseDir + "/exp_8_bloom_metrics.csv", std::ios::app);
        if (!out) {
            spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
            return;
        }

        // hierarchies vector
        std::vector<BloomTree> queryTrees;
        std::vector<std::string> expectedValues;
        std::string expectedValueSuffix = "_value" + std::to_string(dbSize / 2) + std::string(1000, 'a');
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

// ##### Main function ####
int main() {
    const std::string baseDir = "db";
    if (!std::filesystem::exists(baseDir)) {
        std::filesystem::create_directory(baseDir);
    }
    try {
        // run section
        // runExp1(baseDir);
        // runExp2(baseDir);
        // runExp3(baseDir);
        // runExp4(baseDir);
        // runExp5(baseDir);
        // runExp6(baseDir);
        runExp7(baseDir);
        // runExp8(baseDir);
    } catch (const std::exception& e) {
        spdlog::error("[Error] {}", e.what());
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
