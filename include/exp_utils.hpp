#pragma once

#include <future>
#include <map>
#include <string>
#include <vector>

#include "test_params.hpp"

// Forward declarations
class DBManager;
class BloomManager;
class BloomTree;

struct TimingStatistics {
  long long min = 0;
  long long max = 0;
  double median = 0.0;
  double average = 0.0;
};

struct CountStatistics {
  size_t min = 0;
  size_t max = 0;
  double median = 0.0;
  double average = 0.0;
};

struct AggregatedQueryTimings {
  TimingStatistics globalScanTimeStats;
  TimingStatistics hierarchicalMultiTimeStats;
  TimingStatistics hierarchicalSingleTimeStats;

  CountStatistics multiCol_bloomChecksStats;
  CountStatistics multiCol_leafBloomChecksStats;
  CountStatistics multiCol_sstChecksStats;

  CountStatistics singleCol_bloomChecksStats;
  CountStatistics singleCol_leafBloomChecksStats;
  CountStatistics singleCol_sstChecksStats;
};

std::map<std::string, std::vector<std::string>> scanSstFilesAsync(
    const std::vector<std::string>& columns, DBManager& dbManager,
    const TestParams& params);

std::map<std::string, BloomTree> buildHierarchies(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    BloomManager& bloomManager, const TestParams& params);

AggregatedQueryTimings runStandardQueries(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSizeForExpectedValues,  // This might need to become a list or
                                     // generate multiple values
    int numRuns = 10,  // Added numRuns parameter with a default value
    bool skipDbScan = false);

AggregatedQueryTimings runStandardQueriesWithTarget(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numRuns,
    bool skipDbScan, std::vector<std::string> currentExpectedValues);

void writeCsvHeader(const std::string& filename, const std::string& headerLine);

double getProbabilityOfFalsePositive(size_t bloomSize, int numHashFunctions,
                                     size_t itemsPerPartition);

// Helper function to calculate statistics
template <typename T>
TimingStatistics calculateNumericStatistics(const std::vector<T>& values);

template <typename T>
CountStatistics calculateCountStatistics(const std::vector<T>& values);