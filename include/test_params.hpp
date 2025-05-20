#pragma once

#include <string>
#include <cstddef>

struct TestParams {
    std::string dbName;
    int numRecords;
    int bloomTreeRatio;
    int numberOfAttempts;
    size_t itemsPerPartition;
    size_t bloomSize;
    int numHashFunctions;
};
