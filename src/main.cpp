#include <iostream>
#include <string>
#include <vector>
#include <map>

#include "db_manager.hpp"
#include "bloom_manager.hpp"
#include "bloomTree.hpp"
#include "partition_manager.hpp"

#include <spdlog/spdlog.h>
#include <thread>
#include <chrono>

struct TestParams {
    std::string dbNameBase;
    bool withListener;
    int numRecords;
    int ratio;
    int numberOfAttempts;
    int percentageToCheck;
};

void runColumnTest(const TestParams& params, int attemptIndex) {
    std::string dbName = params.dbNameBase + std::to_string(attemptIndex);

    DBManager dbManager;
    BloomManager bloomManager;

    dbManager.openDB(dbName, params.withListener);
    dbManager.insertRecords(params.numRecords);

    std::this_thread::sleep_for(std::chrono::seconds(15));

    std::vector<std::string> columns = {"phone", "mail", "address", "name", "surname"};
    std::map<std::string, bloomTree> hierarchies;

    for (const auto& column : columns) {
        auto sstFiles = dbManager.scanSSTFilesForColumn(dbName, column);
        bloomManager.createBloomValuesForSSTFiles(sstFiles);

        bloomTree hierarchy(params.ratio);
        bloomManager.createHierarchy(sstFiles, hierarchy);
        hierarchies[column] = hierarchy;

        spdlog::info("Hierarchy created for column: {}", column);
    }

    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Example individual column search
        // const std::string value = column + "_value" + std::to_string(i) + std::string(1000, 'a');

    std::string valuePhone = "phone_value456" + std::string(1000, 'a');
    bool foundPhone = dbManager.checkValueInHierarchy(hierarchies["phone"], valuePhone);
    spdlog::info(foundPhone ? "[Info] Value found in phone hierarchy."
                            : "[Info] Value not found in phone hierarchy.");

    // Multi-column search example
    std::string valueName = "namevalue456"+ std::string(1000, 'a');

    bool foundInBoth = dbManager.checkValueAcrossHierarchies(
        hierarchies["phone"], valuePhone, hierarchies["name"], valueName);

    spdlog::info(foundInBoth ? "[Info] Record with specified phone and name found."
                             : "[Info] Record with specified phone and name not found.");

    dbManager.closeDB();
}

void runAllAttempts(const TestParams& params) {
    for (int i = 1; i <= params.numberOfAttempts; ++i) {
        spdlog::info("--- Running column-based attempt {}/{} with ratio={}, numRecords={} ---",
                     i, params.numberOfAttempts, params.ratio, params.numRecords);
        runColumnTest(params, i);
    }
}

int main() {
    try {
        spdlog::set_level(spdlog::level::debug);

        std::vector<TestParams> testSets = {
            {"branch_columns_", true, 1000000, 3, 1, 80}
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
