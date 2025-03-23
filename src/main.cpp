#include <spdlog/spdlog.h>

#include <chrono>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "bloomTree.hpp"
#include "bloom_manager.hpp"
#include "db_manager.hpp"
#include "stopwatch.hpp"

struct TestParams {
    std::string dbName;
    bool compactionLogging;
    int numRecords;
    int bloomTreeRatio;
    int numberOfAttempts;
    double bloomFalsePositiveRate;
    size_t itemsPerPartition;
    size_t bloomSize;
    int numHashFunctions;
};

void runColumnTest(const TestParams& params, int attemptIndex,
                   const std::string& column1, const std::string& value1,
                   const std::string& column2, const std::string& value2) {
    auto dbName = params.dbName + "_" + std::to_string(attemptIndex);
    DBManager dbManager;
    BloomManager bloomManager;
    // const std::vector<std::string> columns = {"phone", "mail", "address", "name", "surname"};
    const std::vector<std::string> columns = {"phone", "mail","name"};

    dbManager.openDB(dbName, params.compactionLogging);
    dbManager.insertRecords(params.numRecords, columns);
    std::this_thread::sleep_for(std::chrono::seconds(5));

    std::map<std::string, BloomTree> hierarchies;
    for (const auto& column : columns) {
        auto sstFiles = dbManager.scanSSTFilesForColumn(dbName, column);
        BloomTree hierarchy = bloomManager.createPartitionedHierarchy(sstFiles, params.itemsPerPartition,
                                                                      params.bloomSize, params.bloomTreeRatio,
                                                                      params.numHashFunctions);

        hierarchies.try_emplace(column, std::move(hierarchy));

        spdlog::info("Hierarchy built for column: {}", column);
    }

    dbManager.noBloomCheckRecordWithTwoColumns(column1, value1, column2, value2);

    dbManager.findRecordInHierarchy(hierarchies.at(column1), value1);
    dbManager.findRecordInHierarchy(hierarchies.at(column2), value2);

    dbManager.findRecordInHierarchies(hierarchies.at(column1), value1, hierarchies.at(column2), value2);

    dbManager.closeDB();
}

int main() {
    try {
        spdlog::set_level(spdlog::level::info);

        //     std::string dbName;
        //    bool compactionLogging;
        //     int numRecords;
        //     int bloomTreeRatio;
        //     int numberOfAttempts;
        //     double bloomFalsePositiveRate;
        //     size_t itemsPerPartition;
        //     size_t bloomSize;
        //     int numHashFunctions;
        TestParams params{
            "column_db_test",
            false,
            1'000'000,
            3,
            1,
            0.01,
            100'000,
            1'000'000,
            6};

        const std::string column1 = "phone";
        const std::string value1 = column1 + "_value654321" + std::string(1000, 'a');

        const std::string column2 = "name";
        const std::string value2 = column2 + "_value654321" + std::string(1000, 'a');

        runColumnTest(params, 1, column1, value1, column2, value2);

    } catch (const std::exception& e) {
        spdlog::error("[Error] {}", e.what());
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
