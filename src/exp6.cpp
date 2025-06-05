#include <spdlog/spdlog.h>

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
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
#include "exp_utils.hpp"
#include "stopwatch.hpp"
#include "test_params.hpp"

extern void clearBloomFilterFiles(const std::string& dbDir);
extern boost::asio::thread_pool globalThreadPool;

void writeExp6BasicTimingsHeaders() {
  writeCsvHeader("csv/exp_6_basic_timings.csv",
                 "numRecords,bloomSize,falsePositiveProbability,"
                 "globalScanTime,hierarchicalSingleTime,hierarchicalMultiTime");
}

void writeExp6BasicChecksHeaders() {
  writeCsvHeader("csv/exp_6_basic_checks.csv",
                 "numRecords,bloomSize,"
                 "multiBloomChecks,multiLeafBloomChecks,multiSSTChecks,"
                 "singleBloomChecks,singleLeafBloomChecks,singleSSTChecks");
}

void writeExp6PerColumnMetricsHeaders() {
  writeCsvHeader("csv/exp_6_per_column_metrics.csv",
                 "numRecords,bloomSize,numColumns,"
                 "multiBloomPerCol,multiLeafPerCol,multiNonLeafPerCol,multiSSTPerCol");
}

void writeExp6RealDataChecksHeaders() {
  writeCsvHeader("csv/exp_6_real_data_checks.csv",
                 "numRecords,bloomSize,realDataPercentage,"
                 "avgMultiBloomChecks,avgMultiLeafBloomChecks,avgMultiNonLeafBloomChecks,avgMultiSSTChecks,"
                 "avgSingleBloomChecks,avgSingleLeafBloomChecks,avgSingleNonLeafBloomChecks,avgSingleSSTChecks,"
                 "avgRealMultiBloomChecks,avgRealMultiSSTChecks,avgFalseMultiBloomChecks,avgFalseMultiSSTChecks");
}

void writeExp6RealDataPerColumnHeaders() {
  writeCsvHeader("csv/exp_6_real_data_per_column.csv",
                 "numRecords,bloomSize,realDataPercentage,numColumns,"
                 "avgMultiBloomPerCol,avgMultiLeafPerCol,avgMultiNonLeafPerCol,avgMultiSSTPerCol,"
                 "avgRealMultiBloomPerCol,avgRealMultiSSTPerCol,avgFalseMultiBloomPerCol,avgFalseMultiSSTPerCol");
}

void writeExp6SizeEfficiencyHeaders() {
  writeCsvHeader("csv/exp_6_size_efficiency.csv",
                 "numRecords,bloomSize,realDataPercentage,falsePositiveProbability,"
                 "avgMultiTime,avgSingleTime,avgMultiBloomPerCol,avgMultiSSTPerCol");
}

void writeExp6TimingComparisonHeaders() {
  writeCsvHeader("csv/exp_6_timing_comparison.csv",
                 "numRecords,bloomSize,realDataPercentage,"
                 "avgRealMultiTime,avgRealSingleTime,avgFalseMultiTime,avgFalseSingleTime,"
                 "avgHierarchicalMultiTime,avgHierarchicalSingleTime");
}

void runExp6(const std::string& dbPath, size_t dbSize, bool skipDbScan) {
  const std::vector<std::string> columns = {"phone", "mail", "address"};
  const std::vector<size_t> bloomSizes = {2000000, 4000000, 8000000};
  const int numQueryRuns = 100;

  // Initialize CSV headers
  writeExp6BasicTimingsHeaders();
  writeExp6BasicChecksHeaders();
  writeExp6PerColumnMetricsHeaders();
  writeExp6RealDataChecksHeaders();
  writeExp6RealDataPerColumnHeaders();
  writeExp6SizeEfficiencyHeaders();
  writeExp6TimingComparisonHeaders();

  DBManager dbManager;
  BloomManager bloomManager;

  for (const auto& bloomSize : bloomSizes) {
    TestParams params = {
        dbPath, static_cast<int>(dbSize), 3, 1, 100000, bloomSize, 3};
    spdlog::info(
        "Exp6: Running experiment for database '{}', bloom size: {} bits",
        params.dbName, bloomSize);

    clearBloomFilterFiles(params.dbName);
    dbManager.openDB(params.dbName);

    std::map<std::string, std::vector<std::string>> columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);

    std::map<std::string, BloomTree> hierarchies =
        buildHierarchies(columnSstFiles, bloomManager, params);

    // Run standard queries first
    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, columns, dbSize, numQueryRuns, skipDbScan);

    double falsePositiveProb = getProbabilityOfFalsePositive(
        params.bloomSize, params.numHashFunctions, params.itemsPerPartition);

    // Write basic performance metrics
    std::ofstream basic_timings("csv/exp_6_basic_timings.csv", std::ios::app);
    if (basic_timings) {
      basic_timings << dbSize << "," << bloomSize << "," << falsePositiveProb << ","
                    << timings.globalScanTimeStats.average << ","
                    << timings.hierarchicalSingleTimeStats.average << ","
                    << timings.hierarchicalMultiTimeStats.average << "\n";
      basic_timings.close();
    }

    std::ofstream basic_checks("csv/exp_6_basic_checks.csv", std::ios::app);
    if (basic_checks) {
      basic_checks << dbSize << "," << bloomSize << ","
                   << timings.multiCol_bloomChecksStats.average << ","
                   << timings.multiCol_leafBloomChecksStats.average << ","
                   << timings.multiCol_sstChecksStats.average << ","
                   << timings.singleCol_bloomChecksStats.average << ","
                   << timings.singleCol_leafBloomChecksStats.average << ","
                   << timings.singleCol_sstChecksStats.average << "\n";
      basic_checks.close();
    }

    std::ofstream per_column_metrics("csv/exp_6_per_column_metrics.csv", std::ios::app);
    if (per_column_metrics) {
      per_column_metrics << dbSize << "," << bloomSize << "," << columns.size() << ","
                         << timings.multiCol_bloomChecksPerColumnStats.average << ","
                         << timings.multiCol_leafBloomChecksPerColumnStats.average << ","
                         << timings.multiCol_nonLeafBloomChecksPerColumnStats.average << ","
                         << timings.multiCol_sstChecksPerColumnStats.average << "\n";
      per_column_metrics.close();
    }

    // Run comprehensive analysis for real data percentage studies
    const int numQueriesPerScenario = 100;
    
    spdlog::info("Exp6: Running comprehensive analysis for {} columns with {} queries per scenario", 
                 columns.size(), numQueriesPerScenario);
    std::vector<AccumulatedQueryMetrics> comprehensiveResults = runComprehensiveQueryAnalysis(
        dbManager, hierarchies, columns, dbSize, numQueriesPerScenario);
    
    spdlog::info("Exp6: Generated {} comprehensive analysis results for {} columns", 
                 comprehensiveResults.size(), columns.size());

    // Write comprehensive analysis results to focused CSV files
    std::ofstream real_data_checks("csv/exp_6_real_data_checks.csv", std::ios::app);
    std::ofstream real_data_per_column("csv/exp_6_real_data_per_column.csv", std::ios::app);
    std::ofstream size_efficiency("csv/exp_6_size_efficiency.csv", std::ios::app);
    std::ofstream timing_comparison("csv/exp_6_timing_comparison.csv", std::ios::app);

    for (const auto& result : comprehensiveResults) {
      // Real data checks (15 columns)
      if (real_data_checks) {
        real_data_checks << dbSize << "," << bloomSize << "," << result.realDataPercentage << ","
                         << result.avgMultiBloomChecks << "," << result.avgMultiLeafBloomChecks << ","
                         << result.avgMultiNonLeafBloomChecks << "," << result.avgMultiSSTChecks << ","
                         << result.avgSingleBloomChecks << "," << result.avgSingleLeafBloomChecks << ","
                         << result.avgSingleNonLeafBloomChecks << "," << result.avgSingleSSTChecks << ","
                         << result.avgRealMultiBloomChecks << "," << result.avgRealMultiSSTChecks << ","
                         << result.avgFalseMultiBloomChecks << "," << result.avgFalseMultiSSTChecks << "\n";
      }

      // Real data per-column (12 columns) - only multi-column gets per-column treatment
      if (real_data_per_column) {
        real_data_per_column << dbSize << "," << bloomSize << "," << result.realDataPercentage << "," << result.numColumns << ","
                             << result.avgMultiBloomChecksPerColumn << "," << result.avgMultiLeafBloomChecksPerColumn << ","
                             << result.avgMultiNonLeafBloomChecksPerColumn << "," << result.avgMultiSSTChecksPerColumn << ","
                             << result.avgRealMultiBloomChecksPerColumn << "," << result.avgRealMultiSSTChecksPerColumn << ","
                             << result.avgFalseMultiBloomChecksPerColumn << "," << result.avgFalseMultiSSTChecksPerColumn << "\n";
      }

      // Size efficiency summary (8 columns)
      if (size_efficiency) {
        size_efficiency << dbSize << "," << bloomSize << "," << result.realDataPercentage << "," << falsePositiveProb << ","
                        << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << ","
                        << result.avgMultiBloomChecksPerColumn << "," << result.avgMultiSSTChecksPerColumn << "\n";
      }

      // Timing comparison (9 columns)
      if (timing_comparison) {
        timing_comparison << dbSize << "," << bloomSize << "," << result.realDataPercentage << ","
                         << result.avgRealDataMultiTime << "," << result.avgRealDataSingleTime << ","
                         << result.avgFalseDataMultiTime << "," << result.avgFalseDataSingleTime << ","
                         << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << "\n";
      }
    }

    real_data_checks.close();
    real_data_per_column.close();
    size_efficiency.close();
    timing_comparison.close();

    dbManager.closeDB();
  }
}