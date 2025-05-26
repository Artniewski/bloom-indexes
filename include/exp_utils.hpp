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

struct PatternQueryResult {
  double percent;
  long long hierarchicalMultiTime;
  long long hierarchicalSingleTime;
  size_t multiCol_bloomChecks;
  size_t multiCol_leafBloomChecks;
  size_t multiCol_sstChecks;
  size_t singleCol_bloomChecks;
  size_t singleCol_leafBloomChecks;
  size_t singleCol_sstChecks;
};

struct MixedQueryResult {
  int queryIndex;
  bool isRealData;
  long long hierarchicalMultiTime;
  long long hierarchicalSingleTime;
  size_t multiCol_bloomChecks;
  size_t multiCol_leafBloomChecks;
  size_t multiCol_sstChecks;
  size_t singleCol_bloomChecks;
  size_t singleCol_leafBloomChecks;
  size_t singleCol_sstChecks;
};

struct AccumulatedQueryMetrics {
  double realDataPercentage;
  int totalQueries;
  int realQueries;
  int falseQueries;
  
  // Average timing metrics
  double avgHierarchicalMultiTime;
  double avgHierarchicalSingleTime;
  double avgRealDataMultiTime;
  double avgRealDataSingleTime;
  double avgFalseDataMultiTime;
  double avgFalseDataSingleTime;
  
  // Average check metrics
  double avgMultiBloomChecks;
  double avgMultiLeafBloomChecks;
  double avgMultiSSTChecks;
  double avgSingleBloomChecks;
  double avgSingleLeafBloomChecks;
  double avgSingleSSTChecks;
  
  // Separate averages for real vs false data
  double avgRealMultiBloomChecks;
  double avgRealMultiSSTChecks;
  double avgFalseMultiBloomChecks;
  double avgFalseMultiSSTChecks;
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

// Helper function to generate dynamic patterns based on column count
std::vector<std::vector<bool>> generateDynamicPatterns(size_t numColumns);

// Test function to verify pattern generation (for debugging)
void testPatternGeneration();

// Function to run pattern queries and collect individual results
std::vector<PatternQueryResult> runPatternQueriesWithCsvData(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize);

// Function to run N queries with X% real data and (100-X)% false data
std::vector<MixedQueryResult> runMixedQueriesWithCsvData(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numQueries, 
    double realDataPercentage);

// Function to run comprehensive analysis across multiple real data percentages
std::vector<AccumulatedQueryMetrics> runComprehensiveQueryAnalysis(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numQueriesPerScenario);

void writeCsvHeader(const std::string& filename, const std::string& headerLine);

double getProbabilityOfFalsePositive(size_t bloomSize, int numHashFunctions,
                                     size_t itemsPerPartition);

// Helper function to calculate statistics
template <typename T>
TimingStatistics calculateNumericStatistics(const std::vector<T>& values);

template <typename T>
CountStatistics calculateCountStatistics(const std::vector<T>& values);