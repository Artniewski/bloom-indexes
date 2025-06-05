#include "exp_utils.hpp"

#include <spdlog/spdlog.h>

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <cmath>
#include <fstream>
#include <random>
#include <sstream>
#include <thread>

#include "algorithm.hpp"
#include "bloomTree.hpp"
#include "bloom_manager.hpp"
#include "db_manager.hpp"
#include "stopwatch.hpp"

extern boost::asio::thread_pool globalThreadPool;
extern std::atomic<size_t> gBloomCheckCount;
extern std::atomic<size_t> gLeafBloomCheckCount;
extern std::atomic<size_t> gSSTCheckCount;

std::map<std::string, std::vector<std::string>> scanSstFilesAsync(
    const std::vector<std::string>& columns, DBManager& dbManager,
    const TestParams& params) {
  std::map<std::string, std::vector<std::string>> columnSstFiles;
  std::vector<std::future<std::pair<std::string, std::vector<std::string>>>>
      scanFutures;

  for (const auto& column : columns) {
    std::promise<std::pair<std::string, std::vector<std::string>>> scanPromise;
    scanFutures.push_back(scanPromise.get_future());

    boost::asio::post(globalThreadPool, [column, &dbManager, &params,
                                         promise =
                                             std::move(scanPromise)]() mutable {
      auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, column);
      promise.set_value(std::make_pair(column, std::move(sstFiles)));
    });
  }

  // Wait for all scanning to complete
  for (auto& fut : scanFutures) {
    auto [column, sstFiles] = fut.get();
    columnSstFiles[column] = std::move(sstFiles);
  }
  return columnSstFiles;
}

std::map<std::string, BloomTree> buildHierarchies(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    BloomManager& bloomManager, const TestParams& params) {
  std::map<std::string, BloomTree> hierarchies;
  for (const auto& [column, sstFiles] : columnSstFiles) {
    BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
        sstFiles, params.itemsPerPartition, params.bloomSize,
        params.numHashFunctions, params.bloomTreeRatio);
    spdlog::info("Hierarchy built for column: {}", column);
    hierarchies.try_emplace(column, std::move(hierarchy));
  }
  return hierarchies;
}

void writeCsvHeader(const std::string& filename,
                    const std::string& headerLine) {
  std::ofstream out(filename, std::ios::app);  // Overwrite mode
  if (!out) {
    spdlog::error(
        "Utils: Nie udało się otworzyć pliku '{}' do zapisu nagłówka!",
        filename);
    exit(1);  // Consistent with how other header functions handle errors
  }
  out << headerLine << "\n";
  out.close();
}

double getProbabilityOfFalsePositive(size_t bloomSize, int numHashFunctions,
                                     size_t itemsPerPartition) {
  if (bloomSize == 0) {
    return 1.0;
  }
  double exponent =
      -static_cast<double>(numHashFunctions) * itemsPerPartition / bloomSize;
  double base = 1.0 - std::exp(exponent);
  return std::pow(base, numHashFunctions);
}

template <typename T>
TimingStatistics calculateNumericStatistics(const std::vector<T>& values) {
  if (values.empty()) {
    spdlog::warn(
        "calculateNumericStatistics called with empty vector. Returning zeroed "
        "statistics.");
    return TimingStatistics{};
  }

  std::vector<T> sorted_values = values;
  std::sort(sorted_values.begin(), sorted_values.end());

  TimingStatistics stats;
  stats.min = static_cast<long long>(sorted_values.front());
  stats.max = static_cast<long long>(sorted_values.back());

  if (sorted_values.size() % 2 == 0) {
    stats.median =
        static_cast<double>(sorted_values[sorted_values.size() / 2 - 1] +
                            sorted_values[sorted_values.size() / 2]) /
        2.0;
  } else {
    stats.median = static_cast<double>(sorted_values[sorted_values.size() / 2]);
  }

  stats.average = static_cast<double>(std::accumulate(
                      sorted_values.begin(), sorted_values.end(), 0LL)) /
                  sorted_values.size();
  return stats;
}

template <typename T>
CountStatistics calculateCountStatistics(const std::vector<T>& values) {
  if (values.empty()) {
    spdlog::warn(
        "calculateCountStatistics called with empty vector. Returning zeroed "
        "statistics.");
    return CountStatistics{};
  }

  std::vector<T> sorted_values = values;
  std::sort(sorted_values.begin(), sorted_values.end());

  CountStatistics stats;
  stats.min = static_cast<size_t>(sorted_values.front());
  stats.max = static_cast<size_t>(sorted_values.back());

  if (sorted_values.size() % 2 == 0) {
    stats.median =
        static_cast<double>(sorted_values[sorted_values.size() / 2 - 1] +
                            sorted_values[sorted_values.size() / 2]) /
        2.0;
  } else {
    stats.median = static_cast<double>(sorted_values[sorted_values.size() / 2]);
  }

  // Use 0ULL for size_t accumulation to avoid overflow issues with large counts
  // if T is smaller than size_t and to ensure the type of the sum is large
  // enough.
  unsigned long long sum = 0;
  for (const T& val : sorted_values) {
    sum += val;
  }
  stats.average = static_cast<double>(sum) / sorted_values.size();
  return stats;
}

AggregatedQueryTimings runStandardQueries(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numRuns,
    bool skipDbScan) {
  AggregatedQueryTimings aggregated_timings;
  if (numRuns <= 0) {
    spdlog::warn(
        "runStandardQueries: numRuns is {} (<=0). Returning empty statistics.",
        numRuns);
    return aggregated_timings;
  }

  std::vector<long long> globalScanTimes, hierarchicalMultiTimes,
      hierarchicalSingleTimes;
  std::vector<size_t> multiCol_bloomChecks_vec;
  std::vector<size_t> multiCol_leafBloomChecks_vec;
  std::vector<size_t> multiCol_sstChecks_vec;
  std::vector<size_t> multiCol_nonLeafBloomChecks_vec;
  std::vector<size_t> singleCol_bloomChecks_vec;
  std::vector<size_t> singleCol_leafBloomChecks_vec;
  std::vector<size_t> singleCol_sstChecks_vec;
  std::vector<size_t> singleCol_nonLeafBloomChecks_vec;

  // Reserve space in vectors
  globalScanTimes.reserve(numRuns);
  hierarchicalMultiTimes.reserve(numRuns);
  hierarchicalSingleTimes.reserve(numRuns);
  multiCol_bloomChecks_vec.reserve(numRuns);
  multiCol_leafBloomChecks_vec.reserve(numRuns);
  multiCol_sstChecks_vec.reserve(numRuns);
  multiCol_nonLeafBloomChecks_vec.reserve(numRuns);
  singleCol_bloomChecks_vec.reserve(numRuns);
  singleCol_leafBloomChecks_vec.reserve(numRuns);
  singleCol_sstChecks_vec.reserve(numRuns);
  singleCol_nonLeafBloomChecks_vec.reserve(numRuns);

  // --- Setup for generating expected values ---
  // This part is outside the loop as queryTrees are constant for all runs.
  if (hierarchies.empty() || columns.empty()) {
    spdlog::warn(
        "runStandardQueries: Hierarchies map or columns vector is empty, "
        "skipping query execution.");
    return aggregated_timings;
  }

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runStandardQueries: Hierarchy for column '{}' not found. Skipping "
          "query execution.",
          column);
      return AggregatedQueryTimings{};
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runStandardQueries: No query trees were prepared, possibly due to "
        "missing hierarchies. Skipping query execution.");
    return aggregated_timings;
  }

  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<size_t> distribution(1, dbSize);

  StopWatch stopwatch;
  std::vector<std::string> currentExpectedValues;
  long long globalScanTime = 0;
  for (int i = 0; i < numRuns; ++i) {
    currentExpectedValues.clear();
    currentExpectedValues.reserve(columns.size());

    size_t currentId = distribution(generator);
    std::string currentExpectedValueSuffix =
        "_value" + std::to_string(currentId);

    spdlog::info("Run {}: Using expected value suffix: {}", i + 1,
                 currentExpectedValueSuffix);

    for (const auto& column : columns) {
      currentExpectedValues.push_back(column + currentExpectedValueSuffix);
    }

    // --- Global Scan Query ---
    if (!skipDbScan && i == 0) {
      stopwatch.start();
      [[maybe_unused]] std::vector<std::string> globalMatches =
          dbManager.scanForRecordsInColumns(columns, currentExpectedValues);
      stopwatch.stop();
      globalScanTime = stopwatch.elapsedMicros();
    } else {
      globalScanTime = 0;
    }
    globalScanTimes.push_back(globalScanTime);

    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees, currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    hierarchicalMultiTimes.push_back(stopwatch.elapsedMicros());
    multiCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    multiCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    multiCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    multiCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    // --- Hierarchical Single Column Query ---
    // Ensure queryTrees[0] is valid before dereferencing. Already checked by
    // queryTrees.empty()
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    hierarchicalSingleTimes.push_back(stopwatch.elapsedMicros());
    singleCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    singleCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    singleCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    singleCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Calculate statistics
  aggregated_timings.globalScanTimeStats =
      calculateNumericStatistics(globalScanTimes);
  aggregated_timings.hierarchicalMultiTimeStats =
      calculateNumericStatistics(hierarchicalMultiTimes);
  aggregated_timings.hierarchicalSingleTimeStats =
      calculateNumericStatistics(hierarchicalSingleTimes);

  aggregated_timings.multiCol_bloomChecksStats =
      calculateCountStatistics(multiCol_bloomChecks_vec);
  aggregated_timings.multiCol_leafBloomChecksStats =
      calculateCountStatistics(multiCol_leafBloomChecks_vec);
  aggregated_timings.multiCol_sstChecksStats =
      calculateCountStatistics(multiCol_sstChecks_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksStats =
      calculateCountStatistics(multiCol_nonLeafBloomChecks_vec);

  aggregated_timings.singleCol_bloomChecksStats =
      calculateCountStatistics(singleCol_bloomChecks_vec);
  aggregated_timings.singleCol_leafBloomChecksStats =
      calculateCountStatistics(singleCol_leafBloomChecks_vec);
  aggregated_timings.singleCol_sstChecksStats =
      calculateCountStatistics(singleCol_sstChecks_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksStats =
      calculateCountStatistics(singleCol_nonLeafBloomChecks_vec);

  // Store number of columns for reference
  aggregated_timings.numColumns = columns.size();

  // Calculate per-column statistics (divide by column count)
  double numCols = static_cast<double>(columns.size());
  std::vector<double> multiCol_bloomChecksPerColumn_vec, multiCol_leafBloomChecksPerColumn_vec, 
                      multiCol_sstChecksPerColumn_vec, multiCol_nonLeafBloomChecksPerColumn_vec;
  std::vector<double> singleCol_bloomChecksPerColumn_vec, singleCol_leafBloomChecksPerColumn_vec, 
                      singleCol_sstChecksPerColumn_vec, singleCol_nonLeafBloomChecksPerColumn_vec;

  for (size_t i = 0; i < multiCol_bloomChecks_vec.size(); ++i) {
    multiCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_bloomChecks_vec[i]) / numCols);
    multiCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_leafBloomChecks_vec[i]) / numCols);
    multiCol_sstChecksPerColumn_vec.push_back(static_cast<double>(multiCol_sstChecks_vec[i]) / numCols);
    multiCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_nonLeafBloomChecks_vec[i]) / numCols);

    singleCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_bloomChecks_vec[i]) );
    singleCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_leafBloomChecks_vec[i]) );
    singleCol_sstChecksPerColumn_vec.push_back(static_cast<double>(singleCol_sstChecks_vec[i]) );
    singleCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_nonLeafBloomChecks_vec[i]) );
  }

  aggregated_timings.multiCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_bloomChecksPerColumn_vec);
  aggregated_timings.multiCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.multiCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_sstChecksPerColumn_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_nonLeafBloomChecksPerColumn_vec);

  aggregated_timings.singleCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_bloomChecksPerColumn_vec);
  aggregated_timings.singleCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.singleCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_sstChecksPerColumn_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_nonLeafBloomChecksPerColumn_vec);

  return aggregated_timings;
}

AggregatedQueryTimings runStandardQueriesWithTarget(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numRuns,
    bool skipDbScan, std::vector<std::string> currentExpectedValues) {
  AggregatedQueryTimings aggregated_timings;

  std::vector<long long> globalScanTimes, hierarchicalMultiTimes,
      hierarchicalSingleTimes;
  std::vector<size_t> multiCol_bloomChecks_vec;
  std::vector<size_t> multiCol_leafBloomChecks_vec;
  std::vector<size_t> multiCol_sstChecks_vec;
  std::vector<size_t> multiCol_nonLeafBloomChecks_vec;
  std::vector<size_t> singleCol_bloomChecks_vec;
  std::vector<size_t> singleCol_leafBloomChecks_vec;
  std::vector<size_t> singleCol_sstChecks_vec;
  std::vector<size_t> singleCol_nonLeafBloomChecks_vec;

  // Reserve space in vectors
  globalScanTimes.reserve(numRuns);
  hierarchicalMultiTimes.reserve(numRuns);
  hierarchicalSingleTimes.reserve(numRuns);
  multiCol_bloomChecks_vec.reserve(numRuns);
  multiCol_leafBloomChecks_vec.reserve(numRuns);
  multiCol_sstChecks_vec.reserve(numRuns);
  multiCol_nonLeafBloomChecks_vec.reserve(numRuns);
  singleCol_bloomChecks_vec.reserve(numRuns);
  singleCol_leafBloomChecks_vec.reserve(numRuns);
  singleCol_sstChecks_vec.reserve(numRuns);
  singleCol_nonLeafBloomChecks_vec.reserve(numRuns);

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runStandardQueries: Hierarchy for column '{}' not found. Skipping "
          "query execution.",
          column);
      return AggregatedQueryTimings{};
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runStandardQueries: No query trees were prepared, possibly due to "
        "missing hierarchies. Skipping query execution.");
    return aggregated_timings;
  }

  StopWatch stopwatch;
  long long globalScanTime = 0;
  for (int i = 0; i < numRuns; ++i) {
    // --- Global Scan Query ---
    if (!skipDbScan && i == 0) {
      stopwatch.start();
      [[maybe_unused]] std::vector<std::string> globalMatches =
          dbManager.scanForRecordsInColumns(columns, currentExpectedValues);
      stopwatch.stop();
      globalScanTime = stopwatch.elapsedMicros();
    } else {
      globalScanTime = 0;
    }
    globalScanTimes.push_back(globalScanTime);

    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees, currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    hierarchicalMultiTimes.push_back(stopwatch.elapsedMicros());
    multiCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    multiCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    multiCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    multiCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    // --- Hierarchical Single Column Query ---
    // Ensure queryTrees[0] is valid before dereferencing. Already checked by
    // queryTrees.empty()
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    hierarchicalSingleTimes.push_back(stopwatch.elapsedMicros());
    singleCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    singleCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    singleCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    singleCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Calculate statistics
  aggregated_timings.globalScanTimeStats =
      calculateNumericStatistics(globalScanTimes);
  aggregated_timings.hierarchicalMultiTimeStats =
      calculateNumericStatistics(hierarchicalMultiTimes);
  aggregated_timings.hierarchicalSingleTimeStats =
      calculateNumericStatistics(hierarchicalSingleTimes);

  aggregated_timings.multiCol_bloomChecksStats =
      calculateCountStatistics(multiCol_bloomChecks_vec);
  aggregated_timings.multiCol_leafBloomChecksStats =
      calculateCountStatistics(multiCol_leafBloomChecks_vec);
  aggregated_timings.multiCol_sstChecksStats =
      calculateCountStatistics(multiCol_sstChecks_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksStats =
      calculateCountStatistics(multiCol_nonLeafBloomChecks_vec);

  aggregated_timings.singleCol_bloomChecksStats =
      calculateCountStatistics(singleCol_bloomChecks_vec);
  aggregated_timings.singleCol_leafBloomChecksStats =
      calculateCountStatistics(singleCol_leafBloomChecks_vec);
  aggregated_timings.singleCol_sstChecksStats =
      calculateCountStatistics(singleCol_sstChecks_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksStats =
      calculateCountStatistics(singleCol_nonLeafBloomChecks_vec);

  // Store number of columns for reference
  aggregated_timings.numColumns = columns.size();

  // Calculate per-column statistics (divide by column count)
  double numCols = static_cast<double>(columns.size());
  std::vector<double> multiCol_bloomChecksPerColumn_vec, multiCol_leafBloomChecksPerColumn_vec, 
                      multiCol_sstChecksPerColumn_vec, multiCol_nonLeafBloomChecksPerColumn_vec;
  std::vector<double> singleCol_bloomChecksPerColumn_vec, singleCol_leafBloomChecksPerColumn_vec, 
                      singleCol_sstChecksPerColumn_vec, singleCol_nonLeafBloomChecksPerColumn_vec;

  for (size_t i = 0; i < multiCol_bloomChecks_vec.size(); ++i) {
    multiCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_bloomChecks_vec[i]) / numCols);
    multiCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_leafBloomChecks_vec[i]) / numCols);
    multiCol_sstChecksPerColumn_vec.push_back(static_cast<double>(multiCol_sstChecks_vec[i]) / numCols);
    multiCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_nonLeafBloomChecks_vec[i]) / numCols);

    singleCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_bloomChecks_vec[i]) );
    singleCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_leafBloomChecks_vec[i]) );
    singleCol_sstChecksPerColumn_vec.push_back(static_cast<double>(singleCol_sstChecks_vec[i]) );
    singleCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_nonLeafBloomChecks_vec[i]) );
  }

  aggregated_timings.multiCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_bloomChecksPerColumn_vec);
  aggregated_timings.multiCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.multiCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_sstChecksPerColumn_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_nonLeafBloomChecksPerColumn_vec);

  aggregated_timings.singleCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_bloomChecksPerColumn_vec);
  aggregated_timings.singleCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.singleCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_sstChecksPerColumn_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_nonLeafBloomChecksPerColumn_vec);

  return aggregated_timings;
}

// Helper function to generate dynamic patterns based on column count
// Generates patterns where we gradually increase the number of existing values
// (t) from left to right: For 4 columns: [n,n,n,n], [t,n,n,n], [t,t,n,n],
// [t,t,t,n], [t,t,t,t]
std::vector<std::vector<bool>> generateDynamicPatterns(size_t numColumns) {
  std::vector<std::vector<bool>> patterns;

  for (size_t numExisting = 0; numExisting <= numColumns; ++numExisting) {
    std::vector<bool> pattern(numColumns, false);  // Start with all n (false)

    // Set first numExisting columns to true (existing values)
    for (size_t i = 0; i < numExisting; ++i) {
      pattern[i] = true;
    }

    patterns.push_back(pattern);
  }

  return patterns;
}

void testPatternGeneration() {
  spdlog::info("Testing pattern generation:");

  for (size_t numCols = 2; numCols <= 5; ++numCols) {
    spdlog::info("For {} columns:", numCols);
    auto patterns = generateDynamicPatterns(numCols);

    for (size_t i = 0; i < patterns.size(); ++i) {
      std::string patternStr = "[";
      for (size_t j = 0; j < patterns[i].size(); ++j) {
        patternStr += (patterns[i][j] ? "t" : "n");
        if (j < patterns[i].size() - 1) patternStr += ",";
      }
      patternStr += "]";

      size_t existingCount = 0;
      for (bool val : patterns[i]) {
        if (val) existingCount++;
      }
      double percentage = static_cast<double>(existingCount) / numCols * 100.0;

      spdlog::info("  Pattern {}: {} ({}% existing)", i, patternStr,
                   percentage);
    }
  }
}

std::vector<PatternQueryResult> runPatternQueriesWithCsvData(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize) {
  std::vector<PatternQueryResult> results;

  // Setup query trees
  if (hierarchies.empty() || columns.empty()) {
    spdlog::warn(
        "runPatternQueriesWithCsvData: Hierarchies map or "
        "columns vector is empty, skipping query execution.");
    return results;
  }

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runPatternQueriesWithCsvData: Hierarchy for column "
          "'{}' not found. Skipping query execution.",
          column);
      return results;
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runPatternQueriesWithCsvData: No query trees were "
        "prepared, possibly due to missing hierarchies. Skipping query "
        "execution.");
    return results;
  }

  // Define non-existing record patterns
  // t = existing value, n = non-existing value
  std::vector<std::vector<bool>> patterns =
      generateDynamicPatterns(columns.size());

  // Log the generated patterns for debugging
  spdlog::info("Generated {} patterns for {} columns:", patterns.size(),
               columns.size());
  for (size_t i = 0; i < patterns.size(); ++i) {
    std::string patternStr = "[";
    for (size_t j = 0; j < patterns[i].size(); ++j) {
      patternStr += (patterns[i][j] ? "t" : "n");
      if (j < patterns[i].size() - 1) patternStr += ",";
    }
    patternStr += "]";
    spdlog::info("Pattern {}: {}", i, patternStr);
  }

  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<size_t> distribution(1, dbSize);

  StopWatch stopwatch;
  std::vector<std::string> currentExpectedValues;
  results.reserve(patterns.size());

  for (int i = 0; i < patterns.size(); ++i) {
    currentExpectedValues.clear();
    currentExpectedValues.reserve(columns.size());

    const auto& pattern = patterns[i];

    size_t existingId = distribution(generator);

    std::string existingValueSuffix = "_value" + std::to_string(existingId);
    std::string nonExistingValueSuffix = "_wrong" + std::to_string(existingId);

    size_t existingCount = 0;
    for (size_t colIdx = 0; colIdx < columns.size() && colIdx < pattern.size();
         ++colIdx) {
      if (pattern[colIdx]) {
        existingCount++;
      }
    }

    double percentageExisting =
        static_cast<double>(existingCount) / columns.size() * 100.0;

    spdlog::info("Run: Using pattern index {} with {}% existing columns", i,
                 percentageExisting);

    // Generate values based on pattern
    for (size_t colIdx = 0; colIdx < columns.size() && colIdx < pattern.size();
         ++colIdx) {
      if (pattern[colIdx]) {
        // existing value
        currentExpectedValues.push_back(columns[colIdx] + existingValueSuffix);
      } else {
        // non-existing value
        currentExpectedValues.push_back(columns[colIdx] +
                                        nonExistingValueSuffix);
      }
    }

    PatternQueryResult result;
    result.percent = percentageExisting;

    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees, currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    result.hierarchicalMultiTime = stopwatch.elapsedMicros();
    result.multiCol_bloomChecks = gBloomCheckCount.load();
    result.multiCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.multiCol_sstChecks = gSSTCheckCount.load();

    // Calculate derived metrics
    result.multiCol_nonLeafBloomChecks = result.multiCol_bloomChecks - result.multiCol_leafBloomChecks;
    
    // Calculate per-column averages
    double numCols = static_cast<double>(columns.size());
    result.multiCol_bloomChecksPerColumn = static_cast<double>(result.multiCol_bloomChecks) / numCols;
    result.multiCol_leafBloomChecksPerColumn = static_cast<double>(result.multiCol_leafBloomChecks) / numCols;
    result.multiCol_sstChecksPerColumn = static_cast<double>(result.multiCol_sstChecks) / numCols;
    result.multiCol_nonLeafBloomChecksPerColumn = static_cast<double>(result.multiCol_nonLeafBloomChecks) / numCols;

    // --- Hierarchical Single Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    result.hierarchicalSingleTime = stopwatch.elapsedMicros();
    result.singleCol_bloomChecks = gBloomCheckCount.load();
    result.singleCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.singleCol_sstChecks = gSSTCheckCount.load();

    // Calculate derived metrics
    result.singleCol_nonLeafBloomChecks = result.singleCol_bloomChecks - result.singleCol_leafBloomChecks;
    
    // Calculate per-column averages
    result.singleCol_bloomChecksPerColumn = static_cast<double>(result.singleCol_bloomChecks) ;
    result.singleCol_leafBloomChecksPerColumn = static_cast<double>(result.singleCol_leafBloomChecks) ;
    result.singleCol_sstChecksPerColumn = static_cast<double>(result.singleCol_sstChecks) ;
    result.singleCol_nonLeafBloomChecksPerColumn = static_cast<double>(result.singleCol_nonLeafBloomChecks) ;

    results.push_back(result);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  return results;
}

std::vector<MixedQueryResult> runMixedQueriesWithCsvData(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numQueries, 
    double realDataPercentage) {
  std::vector<MixedQueryResult> results;

  // Setup query trees
  if (hierarchies.empty() || columns.empty()) {
    spdlog::warn(
        "runMixedQueriesWithCsvData: Hierarchies map or "
        "columns vector is empty, skipping query execution.");
    return results;
  }

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runMixedQueriesWithCsvData: Hierarchy for column "
          "'{}' not found. Skipping query execution.",
          column);
      return results;
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runMixedQueriesWithCsvData: No query trees were "
        "prepared, possibly due to missing hierarchies. Skipping query "
        "execution.");
    return results;
  }

  // Calculate how many queries should have real data
  int numRealQueries = static_cast<int>(std::round(numQueries * realDataPercentage / 100.0));
  int numFalseQueries = numQueries - numRealQueries;

  spdlog::info("runMixedQueriesWithCsvData: Running {} total queries: {} with real data ({}%), {} with false data ({}%)",
               numQueries, numRealQueries, realDataPercentage, numFalseQueries, 100.0 - realDataPercentage);

  // Generate permutation patterns for columns (focusing on cases where first column is true)
  std::vector<std::vector<bool>> patterns = generateDynamicPatterns(columns.size());
  
  spdlog::info("runMixedQueriesWithCsvData: Generated {} permutation patterns for {} columns", 
               patterns.size(), columns.size());

  std::vector<bool> isRealDataQuery(numQueries, false);
  for (int i = 0; i < numRealQueries; ++i) {
    isRealDataQuery[i] = true;
  }

  // Shuffle
  std::random_device rd;
  std::mt19937 generator(rd());
  std::shuffle(isRealDataQuery.begin(), isRealDataQuery.end(), generator);

  std::uniform_int_distribution<size_t> distribution(1, dbSize);
  StopWatch stopwatch;
  std::vector<std::string> currentExpectedValues;
  results.reserve(numQueries);

  for (int queryIdx = 0; queryIdx < numQueries; ++queryIdx) {
    currentExpectedValues.clear();
    currentExpectedValues.reserve(columns.size());

    bool useRealData = isRealDataQuery[queryIdx];
    std::vector<bool> pattern;
    
    if (useRealData) {
      // Use the last pattern (all true) for real data
      pattern = patterns.back();
    } else {
      // Use one of the permutation patterns (except the last one) for false data
      size_t patternIndex = queryIdx % (patterns.size() - 1);
      pattern = patterns[patternIndex];
    }
    
    size_t randomId = distribution(generator);

    std::string existingValueSuffix = "_value" + std::to_string(randomId);
    std::string nonExistingValueSuffix = "_wrong" + std::to_string(randomId);

    // Count existing columns for logging
    size_t existingCount = 0;
    for (bool val : pattern) {
      if (val) existingCount++;
    }
    double percentageExisting = static_cast<double>(existingCount) / columns.size() * 100.0;

    if (useRealData) {
      spdlog::info("Query {}: Using REAL data (all true pattern) with ID {}", 
                   queryIdx + 1, randomId);
    } else {
      size_t patternIndex = queryIdx % (patterns.size() - 1);
      spdlog::info("Query {}: Using FALSE data (pattern {} - {}% existing) with ID {}", 
                   queryIdx + 1, patternIndex, percentageExisting, randomId);
    }

    // Generate values based on pattern
    for (size_t colIdx = 0; colIdx < columns.size() && colIdx < pattern.size(); ++colIdx) {
      if (pattern[colIdx]) {
        // existing value
        currentExpectedValues.push_back(columns[colIdx] + existingValueSuffix);
      } else {
        // non-existing value
        currentExpectedValues.push_back(columns[colIdx] + nonExistingValueSuffix);
      }
    }

    MixedQueryResult result;
    result.queryIndex = queryIdx;
    result.isRealData = useRealData;

    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees, currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    result.hierarchicalMultiTime = stopwatch.elapsedMicros();
    result.multiCol_bloomChecks = gBloomCheckCount.load();
    result.multiCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.multiCol_sstChecks = gSSTCheckCount.load();

    // Calculate derived metrics
    result.multiCol_nonLeafBloomChecks = result.multiCol_bloomChecks - result.multiCol_leafBloomChecks;
    
    // Calculate per-column averages
    double numCols = static_cast<double>(columns.size());
    result.multiCol_bloomChecksPerColumn = static_cast<double>(result.multiCol_bloomChecks) / numCols;
    result.multiCol_leafBloomChecksPerColumn = static_cast<double>(result.multiCol_leafBloomChecks) / numCols;
    result.multiCol_sstChecksPerColumn = static_cast<double>(result.multiCol_sstChecks) / numCols;
    result.multiCol_nonLeafBloomChecksPerColumn = static_cast<double>(result.multiCol_nonLeafBloomChecks) / numCols;

    // --- Hierarchical Single Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    result.hierarchicalSingleTime = stopwatch.elapsedMicros();
    result.singleCol_bloomChecks = gBloomCheckCount.load();
    result.singleCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.singleCol_sstChecks = gSSTCheckCount.load();

    // Calculate derived metrics
    result.singleCol_nonLeafBloomChecks = result.singleCol_bloomChecks - result.singleCol_leafBloomChecks;
    
    // Calculate per-column averages
    result.singleCol_bloomChecksPerColumn = static_cast<double>(result.singleCol_bloomChecks) ;
    result.singleCol_leafBloomChecksPerColumn = static_cast<double>(result.singleCol_leafBloomChecks) ;
    result.singleCol_sstChecksPerColumn = static_cast<double>(result.singleCol_sstChecks) ;
    result.singleCol_nonLeafBloomChecksPerColumn = static_cast<double>(result.singleCol_nonLeafBloomChecks) ;

    results.push_back(result);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  return results;
}

std::vector<AccumulatedQueryMetrics> runComprehensiveQueryAnalysis(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numQueriesPerScenario) {
  
  std::vector<AccumulatedQueryMetrics> accumulatedResults;
  
  // Define the real data percentages to test
  std::vector<double> realDataPercentages = {0.0, 20.0, 40.0, 60.0, 80.0, 100.0};
  
  spdlog::info("runComprehensiveQueryAnalysis: Starting comprehensive analysis with {} queries per scenario", 
               numQueriesPerScenario);
  
  for (double percentage : realDataPercentages) {
    spdlog::info("Running scenario with {}% real data", percentage);
    
    // Run mixed queries for this percentage
    std::vector<MixedQueryResult> results = runMixedQueriesWithCsvData(
        dbManager, hierarchies, columns, dbSize, numQueriesPerScenario, percentage);
    
    if (results.empty()) {
      spdlog::warn("No results returned for {}% real data scenario", percentage);
      continue;
    }
    
    // Accumulate metrics
    AccumulatedQueryMetrics metrics;
    metrics.realDataPercentage = percentage;
    metrics.totalQueries = results.size();
    metrics.realQueries = 0;
    metrics.falseQueries = 0;
    metrics.numColumns = columns.size();
    
    // Initialize sums for averaging
    long long totalMultiTime = 0, totalSingleTime = 0;
    long long realMultiTimeSum = 0, realSingleTimeSum = 0;
    long long falseMultiTimeSum = 0, falseSingleTimeSum = 0;
    
    size_t totalMultiBloomChecks = 0, totalMultiLeafBloomChecks = 0, totalMultiSSTChecks = 0;
    size_t totalSingleBloomChecks = 0, totalSingleLeafBloomChecks = 0, totalSingleSSTChecks = 0;
    size_t totalMultiNonLeafBloomChecks = 0, totalSingleNonLeafBloomChecks = 0;
    
    size_t realMultiBloomChecksSum = 0, realMultiSSTChecksSum = 0;
    size_t falseMultiBloomChecksSum = 0, falseMultiSSTChecksSum = 0;
    
    // Process each query result
    for (const auto& result : results) {
      totalMultiTime += result.hierarchicalMultiTime;
      totalSingleTime += result.hierarchicalSingleTime;
      
      totalMultiBloomChecks += result.multiCol_bloomChecks;
      totalMultiLeafBloomChecks += result.multiCol_leafBloomChecks;
      totalMultiSSTChecks += result.multiCol_sstChecks;
      totalMultiNonLeafBloomChecks += result.multiCol_nonLeafBloomChecks;
      totalSingleBloomChecks += result.singleCol_bloomChecks;
      totalSingleLeafBloomChecks += result.singleCol_leafBloomChecks;
      totalSingleSSTChecks += result.singleCol_sstChecks;
      totalSingleNonLeafBloomChecks += result.singleCol_nonLeafBloomChecks;
      
      if (result.isRealData) {
        metrics.realQueries++;
        realMultiTimeSum += result.hierarchicalMultiTime;
        realSingleTimeSum += result.hierarchicalSingleTime;
        realMultiBloomChecksSum += result.multiCol_bloomChecks;
        realMultiSSTChecksSum += result.multiCol_sstChecks;
      } else {
        metrics.falseQueries++;
        falseMultiTimeSum += result.hierarchicalMultiTime;
        falseSingleTimeSum += result.hierarchicalSingleTime;
        falseMultiBloomChecksSum += result.multiCol_bloomChecks;
        falseMultiSSTChecksSum += result.multiCol_sstChecks;
      }
    }
    
    // Calculate averages
    metrics.avgHierarchicalMultiTime = static_cast<double>(totalMultiTime) / metrics.totalQueries;
    metrics.avgHierarchicalSingleTime = static_cast<double>(totalSingleTime) / metrics.totalQueries;
    
    metrics.avgMultiBloomChecks = static_cast<double>(totalMultiBloomChecks) / metrics.totalQueries;
    metrics.avgMultiLeafBloomChecks = static_cast<double>(totalMultiLeafBloomChecks) / metrics.totalQueries;
    metrics.avgMultiSSTChecks = static_cast<double>(totalMultiSSTChecks) / metrics.totalQueries;
    metrics.avgMultiNonLeafBloomChecks = static_cast<double>(totalMultiNonLeafBloomChecks) / metrics.totalQueries;
    metrics.avgSingleBloomChecks = static_cast<double>(totalSingleBloomChecks) / metrics.totalQueries;
    metrics.avgSingleLeafBloomChecks = static_cast<double>(totalSingleLeafBloomChecks) / metrics.totalQueries;
    metrics.avgSingleSSTChecks = static_cast<double>(totalSingleSSTChecks) / metrics.totalQueries;
    metrics.avgSingleNonLeafBloomChecks = static_cast<double>(totalSingleNonLeafBloomChecks) / metrics.totalQueries;
    
    // Calculate per-column averages (dividing by column count)
    double numCols = static_cast<double>(metrics.numColumns);
    metrics.avgMultiBloomChecksPerColumn = metrics.avgMultiBloomChecks / numCols;
    metrics.avgMultiLeafBloomChecksPerColumn = metrics.avgMultiLeafBloomChecks / numCols;
    metrics.avgMultiSSTChecksPerColumn = metrics.avgMultiSSTChecks / numCols;
    metrics.avgMultiNonLeafBloomChecksPerColumn = metrics.avgMultiNonLeafBloomChecks / numCols;
    metrics.avgSingleBloomChecksPerColumn = metrics.avgSingleBloomChecks ;
    metrics.avgSingleLeafBloomChecksPerColumn = metrics.avgSingleLeafBloomChecks ;
    metrics.avgSingleSSTChecksPerColumn = metrics.avgSingleSSTChecks ;
    metrics.avgSingleNonLeafBloomChecksPerColumn = metrics.avgSingleNonLeafBloomChecks ;
    
    // Calculate separate averages for real vs false data
    if (metrics.realQueries > 0) {
      metrics.avgRealDataMultiTime = static_cast<double>(realMultiTimeSum) / metrics.realQueries;
      metrics.avgRealDataSingleTime = static_cast<double>(realSingleTimeSum) / metrics.realQueries;
      metrics.avgRealMultiBloomChecks = static_cast<double>(realMultiBloomChecksSum) / metrics.realQueries;
      metrics.avgRealMultiSSTChecks = static_cast<double>(realMultiSSTChecksSum) / metrics.realQueries;
      metrics.avgRealMultiBloomChecksPerColumn = metrics.avgRealMultiBloomChecks / numCols;
      metrics.avgRealMultiSSTChecksPerColumn = metrics.avgRealMultiSSTChecks / numCols;
    } else {
      metrics.avgRealDataMultiTime = 0.0;
      metrics.avgRealDataSingleTime = 0.0;
      metrics.avgRealMultiBloomChecks = 0.0;
      metrics.avgRealMultiSSTChecks = 0.0;
      metrics.avgRealMultiBloomChecksPerColumn = 0.0;
      metrics.avgRealMultiSSTChecksPerColumn = 0.0;
    }
    
    if (metrics.falseQueries > 0) {
      metrics.avgFalseDataMultiTime = static_cast<double>(falseMultiTimeSum) / metrics.falseQueries;
      metrics.avgFalseDataSingleTime = static_cast<double>(falseSingleTimeSum) / metrics.falseQueries;
      metrics.avgFalseMultiBloomChecks = static_cast<double>(falseMultiBloomChecksSum) / metrics.falseQueries;
      metrics.avgFalseMultiSSTChecks = static_cast<double>(falseMultiSSTChecksSum) / metrics.falseQueries;
      metrics.avgFalseMultiBloomChecksPerColumn = metrics.avgFalseMultiBloomChecks / numCols;
      metrics.avgFalseMultiSSTChecksPerColumn = metrics.avgFalseMultiSSTChecks / numCols;
    } else {
      metrics.avgFalseDataMultiTime = 0.0;
      metrics.avgFalseDataSingleTime = 0.0;
      metrics.avgFalseMultiBloomChecks = 0.0;
      metrics.avgFalseMultiSSTChecks = 0.0;
      metrics.avgFalseMultiBloomChecksPerColumn = 0.0;
      metrics.avgFalseMultiSSTChecksPerColumn = 0.0;
    }
    
    accumulatedResults.push_back(metrics);
    
    spdlog::info("Scenario {}% complete: {} real queries (avg: {:.2f}μs), {} false queries (avg: {:.2f}μs)",
                 percentage, metrics.realQueries, metrics.avgRealDataMultiTime, 
                 metrics.falseQueries, metrics.avgFalseDataMultiTime);
  }
  
  spdlog::info("Comprehensive analysis completed with {} scenarios", accumulatedResults.size());
  return accumulatedResults;
}