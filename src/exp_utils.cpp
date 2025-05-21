#include "exp_utils.hpp"

#include <spdlog/spdlog.h>

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <cmath>
#include <fstream>
#include <random>

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
  std::vector<size_t> singleCol_bloomChecks_vec;
  std::vector<size_t> singleCol_leafBloomChecks_vec;
  std::vector<size_t> singleCol_sstChecks_vec;

  // Reserve space in vectors
  globalScanTimes.reserve(numRuns);
  hierarchicalMultiTimes.reserve(numRuns);
  hierarchicalSingleTimes.reserve(numRuns);
  multiCol_bloomChecks_vec.reserve(numRuns);
  multiCol_leafBloomChecks_vec.reserve(numRuns);
  multiCol_sstChecks_vec.reserve(numRuns);
  singleCol_bloomChecks_vec.reserve(numRuns);
  singleCol_leafBloomChecks_vec.reserve(numRuns);
  singleCol_sstChecks_vec.reserve(numRuns);

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

  aggregated_timings.singleCol_bloomChecksStats =
      calculateCountStatistics(singleCol_bloomChecks_vec);
  aggregated_timings.singleCol_leafBloomChecksStats =
      calculateCountStatistics(singleCol_leafBloomChecks_vec);
  aggregated_timings.singleCol_sstChecksStats =
      calculateCountStatistics(singleCol_sstChecks_vec);

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
  std::vector<size_t> singleCol_bloomChecks_vec;
  std::vector<size_t> singleCol_leafBloomChecks_vec;
  std::vector<size_t> singleCol_sstChecks_vec;

  // Reserve space in vectors
  globalScanTimes.reserve(numRuns);
  hierarchicalMultiTimes.reserve(numRuns);
  hierarchicalSingleTimes.reserve(numRuns);
  multiCol_bloomChecks_vec.reserve(numRuns);
  multiCol_leafBloomChecks_vec.reserve(numRuns);
  multiCol_sstChecks_vec.reserve(numRuns);
  singleCol_bloomChecks_vec.reserve(numRuns);
  singleCol_leafBloomChecks_vec.reserve(numRuns);
  singleCol_sstChecks_vec.reserve(numRuns);

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

  aggregated_timings.singleCol_bloomChecksStats =
      calculateCountStatistics(singleCol_bloomChecks_vec);
  aggregated_timings.singleCol_leafBloomChecksStats =
      calculateCountStatistics(singleCol_leafBloomChecks_vec);
  aggregated_timings.singleCol_sstChecksStats =
      calculateCountStatistics(singleCol_sstChecks_vec);

  return aggregated_timings;
    }