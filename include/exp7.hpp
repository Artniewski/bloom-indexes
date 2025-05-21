#pragma once

#include <string>
#include <vector>
#include <cstddef>

void runExp7(const std::string& dbPathToUse, size_t dbSizeToUse,
             bool skipDbScan);
void generateRandomIndexes(size_t dbSize, const int numTargetRecords,
                           std::vector<int>& targetRecordIndices);
std::string createPrefixedKeyExp7(int recordIndex, int totalRecords);
