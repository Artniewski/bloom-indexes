#include <iostream>
#include <string>
#include <vector>

#include "db_manager.hpp"
#include "bloom_manager.hpp"
#include "bloomTree.hpp"

#include <spdlog/spdlog.h>

struct TestParams {
    std::string dbNameBase;  // e.g. "testdb"
    bool withListener;       // whether to attach the compaction listener
    int numRecords;          // how many records to insert
    int ratio;     // how many branches per bloomTree level
    int numberOfAttempts;    // how many times to repeat
    int percentageToCheck;   // what percentage of records to check
};

void runSingleTest(const TestParams& params, int attemptIndex) {
    // 1. Build a unique DB name for this attempt
    std::string dbName = params.dbNameBase + std::to_string(attemptIndex);

    // 3. Create managers
    DBManager dbManager;
    BloomManager bloomManager;
    PartitionManager partitionManager;

    // 4. Open DB with or without listener
    dbManager.openDB(dbName, params.withListener);

    // 5. Insert records
    dbManager.insertRecords(params.numRecords);

    std::this_thread::sleep_for(std::chrono::seconds(10));
    // 6. Scan for SST files
    auto sstFiles = dbManager.scanSSTFiles(dbName);

    // 7. Create Bloom filters
    bloomManager.createBloomValuesForSSTFiles(sstFiles);

    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 8. Create Bloom filter hierarchy
    bloomTree hierarchy(params.ratio);
    bloomManager.createHierarchy(sstFiles, hierarchy);
    hierarchy.printTree();

    std::string valueToCheck = "value" + std::to_string((int) (params.numRecords*params.percentageToCheck)/100) + std::string(1000, 'a');
    bool foundHier = dbManager.checkValueInHierarchy(hierarchy, valueToCheck);
    spdlog::info(foundHier 
        ? "[Info] Value was found in the hierarchy."
        : "[Info] Value was not found in the hierarchy.");

// check value without concurrency
    bool foundHierWithoutConcurrency = dbManager.checkValueInHierarchyWithoutConcurrency(hierarchy, valueToCheck);
    spdlog::info(foundHierWithoutConcurrency 
        ? "[Info] Value was found in the hierarchy without concurrency."
        : "[Info] Value was not found in the hierarchy without concurrency.");
    bool foundDB = dbManager.checkValueWithoutBloomFilters(valueToCheck);
    spdlog::info(foundDB 
        ? "[Info] Value was found in the database."
        : "[Info] Value was not found in the database.");

    //new
    // 2. Partition each SST file (using, for example, BlockBased mode).
    auto partitionedFiles = partitionManager.buildPartitionsForSSTFiles(sstFiles, PartitioningMode::FixedSize);
    // 3. Build a partitioned hierarchy.
    bloomTree partitionHierarchy(params.ratio);
    bloomManager.createPartitionedHierarchy(partitionedFiles, partitionHierarchy);

    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 4. Later, to check for a value using the hierarchy:
    if (partitionManager.checkValueInPartitionedHierarchy(valueToCheck, partitionHierarchy)) {
        spdlog::info("Value found in one of the partitions!");
    } else {
        spdlog::info("Value  not found in any partition.");
    }
    //end new


    // 10. Close DB
    dbManager.closeDB();
}

void runAllAttempts(const TestParams& params) {
    for (int i = 1; i <= params.numberOfAttempts; ++i) {
        spdlog::info("--- Running attempt {}/{} with ratio={}, numRecords={} ---",
                     i, params.numberOfAttempts, params.ratio, params.numRecords);
        runSingleTest(params, i);
    }
}

int main() {
    try {
        spdlog::set_level(spdlog::level::debug);

        std::vector<TestParams> testSets = {
          { "branch2_", true, 7000000, 3, 1, 80}
        };
        for (auto& t : testSets) {
          runAllAttempts(t);
        }

    } catch (const std::exception& e) {
        std::cerr << "[Error] " << e.what() << "\n";
        return 1;
    }
    return 0;
}
