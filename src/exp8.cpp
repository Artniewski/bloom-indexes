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

extern void clearBloomFilterFiles(const std::string& dbDir);
extern boost::asio::thread_pool globalThreadPool;

void writeExp8BasicTimingsHeaders() {
  writeCsvHeader("csv/exp_8_basic_timings.csv",
                 "numRecords,numColumns,globalScanTime,hierarchicalSingleTime,hierarchicalMultiTime");
}

void writeExp8BasicChecksHeaders() {
  writeCsvHeader("csv/exp_8_basic_checks.csv",
                 "numRecords,numColumns,"
                 "multiBloomChecks,multiLeafBloomChecks,multiSSTChecks,"
                 "singleBloomChecks,singleLeafBloomChecks,singleSSTChecks");
}

void writeExp8PerColumnMetricsHeaders() {
  writeCsvHeader("csv/exp_8_per_column_metrics.csv",
                 "numRecords,numColumns,"
                 "multiBloomPerCol,multiLeafPerCol,multiNonLeafPerCol,multiSSTPerCol,"
                 "singleBloomPerCol,singleLeafPerCol,singleNonLeafPerCol,singleSSTPerCol");
}

void writeExp8RealDataChecksHeaders() {
  writeCsvHeader("csv/exp_8_real_data_checks.csv",
                 "numRecords,numColumns,realDataPercentage,"
                 "avgMultiBloomChecks,avgMultiLeafBloomChecks,avgMultiNonLeafBloomChecks,avgMultiSSTChecks,"
                 "avgSingleBloomChecks,avgSingleLeafBloomChecks,avgSingleNonLeafBloomChecks,avgSingleSSTChecks,"
                 "avgRealMultiBloomChecks,avgRealMultiSSTChecks,avgFalseMultiBloomChecks,avgFalseMultiSSTChecks");
}

void writeExp8RealDataPerColumnHeaders() {
  writeCsvHeader("csv/exp_8_real_data_per_column.csv",
                 "numRecords,numColumns,realDataPercentage,"
                 "avgMultiBloomPerCol,avgMultiLeafPerCol,avgMultiNonLeafPerCol,avgMultiSSTPerCol,"
                 "avgSingleBloomPerCol,avgSingleLeafPerCol,avgSingleNonLeafPerCol,avgSingleSSTPerCol,"
                 "avgRealMultiBloomPerCol,avgRealMultiSSTPerCol,avgFalseMultiBloomPerCol,avgFalseMultiSSTPerCol");
}

void writeExp8ScalabilityHeaders() {
  writeCsvHeader("csv/exp_8_scalability_summary.csv",
                 "numRecords,numColumns,realDataPercentage,"
                 "avgMultiTime,avgSingleTime,avgMultiBloomPerCol,avgMultiSSTPerCol");
}

void writeExp8TimingComparisonHeaders() {
  writeCsvHeader("csv/exp_8_timing_comparison.csv",
                 "numRecords,numColumns,realDataPercentage,"
                 "avgRealMultiTime,avgRealSingleTime,avgFalseMultiTime,avgFalseSingleTime,"
                 "avgHierarchicalMultiTime,avgHierarchicalSingleTime");
}

void runExp8(std::string baseDir, bool initMode, bool skipDbScan) {
  const int dbSize = 20'000'000;
  const int maxColumns = 12;
  const std::vector<int> numColumnsToTest = {2,4,6,8,10,maxColumns};
  
  const std::string fixedDbName = baseDir + "/exp8_shared_db";
  const int numQueriesPerScenario = 100;

  // Initialize CSV headers
  writeExp8BasicTimingsHeaders();
  writeExp8BasicChecksHeaders(); 
  writeExp8PerColumnMetricsHeaders();
  writeExp8RealDataChecksHeaders();
  writeExp8RealDataPerColumnHeaders();
  writeExp8ScalabilityHeaders();
  writeExp8TimingComparisonHeaders();

  std::vector<std::string> allColumnNames;
  for (int i = 0; i < maxColumns; ++i) {
    allColumnNames.push_back("i_" + std::to_string(i) + "_column");
  }

  DBManager dbManager;
  BloomManager bloomManager;

  // --- Database Initialization (once for all 12 columns) ---
  spdlog::info(
      "ExpBloomMetrics: Initializing shared database '{}' with {} columns if "
      "it doesn't exist.",
      fixedDbName, maxColumns);
  clearBloomFilterFiles(fixedDbName);

  if (std::filesystem::exists(fixedDbName)) {
    spdlog::info(
        "ExpBloomMetrics: Shared database '{}' already exists, skipping "
        "initialization.",
        fixedDbName);
    dbManager.openDB(fixedDbName, allColumnNames);
  } else {
    dbManager.openDB(fixedDbName, allColumnNames);
    dbManager.insertRecords(dbSize, allColumnNames);
    try {
      dbManager.compactAllColumnFamilies(dbSize);
    } catch (const std::exception& e) {
      spdlog::error("Error during initial compaction for '{}': {}", fixedDbName,
                    e.what());
      exit(1);
    }
  }
  dbManager.closeDB();
  // --- End Database Initialization ---

  for (const auto& numCol : numColumnsToTest) {
    std::vector<std::string> currentColumns;
    for (int i = 0; i < numCol; ++i) {
      currentColumns.push_back(allColumnNames[i]);
    }

    spdlog::info("ExpBloomMetrics: Starting iteration for {} columns:", numCol);
    for (const auto& column : currentColumns) {
      spdlog::info("Using Column: {}", column);
    }

    TestParams params = {fixedDbName, dbSize, 3, 1, 100000, 4'000'000, 3};
    spdlog::info(
        "ExpBloomMetrics: Running experiment for database '{}' using {}/{} "
        "columns",
        params.dbName, numCol, maxColumns);

    clearBloomFilterFiles(params.dbName);
    dbManager.openDB(params.dbName, allColumnNames);

    std::map<std::string, std::vector<std::string>> columnSstFiles =
        scanSstFilesAsync(currentColumns, dbManager, params);

    std::map<std::string, BloomTree> hierarchies =
        buildHierarchies(columnSstFiles, bloomManager, params);

    // Run standard queries first
    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, currentColumns, dbSize, 100, skipDbScan);

    // Write basic performance metrics
    std::ofstream basic_timings("csv/exp_8_basic_timings.csv", std::ios::app);
    if (basic_timings) {
      basic_timings << params.numRecords << "," << numCol << ","
                    << timings.globalScanTimeStats.average << ","
                    << timings.hierarchicalSingleTimeStats.average << ","
                    << timings.hierarchicalMultiTimeStats.average << "\n";
      basic_timings.close();
    }

    std::ofstream basic_checks("csv/exp_8_basic_checks.csv", std::ios::app);
    if (basic_checks) {
      basic_checks << params.numRecords << "," << numCol << ","
                   << timings.multiCol_bloomChecksStats.average << ","
                   << timings.multiCol_leafBloomChecksStats.average << ","
                   << timings.multiCol_sstChecksStats.average << ","
                   << timings.singleCol_bloomChecksStats.average << ","
                   << timings.singleCol_leafBloomChecksStats.average << ","
                   << timings.singleCol_sstChecksStats.average << "\n";
      basic_checks.close();
    }

    std::ofstream per_column_metrics("csv/exp_8_per_column_metrics.csv", std::ios::app);
    if (per_column_metrics) {
      per_column_metrics << params.numRecords << "," << numCol << ","
                         << timings.multiCol_bloomChecksPerColumnStats.average << ","
                         << timings.multiCol_leafBloomChecksPerColumnStats.average << ","
                         << timings.multiCol_nonLeafBloomChecksPerColumnStats.average << ","
                         << timings.multiCol_sstChecksPerColumnStats.average << ","
                         << timings.singleCol_bloomChecksPerColumnStats.average << ","
                         << timings.singleCol_leafBloomChecksPerColumnStats.average << ","
                         << timings.singleCol_nonLeafBloomChecksPerColumnStats.average << ","
                         << timings.singleCol_sstChecksPerColumnStats.average << "\n";
      per_column_metrics.close();
    }

    // Run comprehensive analysis for real data percentage studies
    spdlog::info("ExpBloomMetrics: Running comprehensive analysis for {} columns with {} queries per scenario", 
                 numCol, numQueriesPerScenario);
    std::vector<AccumulatedQueryMetrics> comprehensiveResults = runComprehensiveQueryAnalysis(
        dbManager, hierarchies, currentColumns, dbSize, numQueriesPerScenario);
    
    spdlog::info("ExpBloomMetrics: Generated {} comprehensive analysis results for {} columns", 
                 comprehensiveResults.size(), numCol);

    // Write comprehensive analysis results to focused CSV files
    std::ofstream real_data_checks("csv/exp_8_real_data_checks.csv", std::ios::app);
    std::ofstream real_data_per_column("csv/exp_8_real_data_per_column.csv", std::ios::app);
    std::ofstream scalability_summary("csv/exp_8_scalability_summary.csv", std::ios::app);
    std::ofstream timing_comparison("csv/exp_8_timing_comparison.csv", std::ios::app);

    for (const auto& result : comprehensiveResults) {
      // Real data checks (15 columns)
      if (real_data_checks) {
        real_data_checks << params.numRecords << "," << numCol << "," << result.realDataPercentage << ","
                         << result.avgMultiBloomChecks << "," << result.avgMultiLeafBloomChecks << ","
                         << result.avgMultiNonLeafBloomChecks << "," << result.avgMultiSSTChecks << ","
                         << result.avgSingleBloomChecks << "," << result.avgSingleLeafBloomChecks << ","
                         << result.avgSingleNonLeafBloomChecks << "," << result.avgSingleSSTChecks << ","
                         << result.avgRealMultiBloomChecks << "," << result.avgRealMultiSSTChecks << ","
                         << result.avgFalseMultiBloomChecks << "," << result.avgFalseMultiSSTChecks << "\n";
      }

      // Real data per-column (15 columns)
      if (real_data_per_column) {
        real_data_per_column << params.numRecords << "," << numCol << "," << result.realDataPercentage << ","
                             << result.avgMultiBloomChecksPerColumn << "," << result.avgMultiLeafBloomChecksPerColumn << ","
                             << result.avgMultiNonLeafBloomChecksPerColumn << "," << result.avgMultiSSTChecksPerColumn << ","
                             << result.avgSingleBloomChecksPerColumn << "," << result.avgSingleLeafBloomChecksPerColumn << ","
                             << result.avgSingleNonLeafBloomChecksPerColumn << "," << result.avgSingleSSTChecksPerColumn << ","
                             << result.avgRealMultiBloomChecksPerColumn << "," << result.avgRealMultiSSTChecksPerColumn << ","
                             << result.avgFalseMultiBloomChecksPerColumn << "," << result.avgFalseMultiSSTChecksPerColumn << "\n";
      }

      // Scalability summary (7 columns)
      if (scalability_summary) {
        scalability_summary << params.numRecords << "," << numCol << "," << result.realDataPercentage << ","
                            << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << ","
                            << result.avgMultiBloomChecksPerColumn << "," << result.avgMultiSSTChecksPerColumn << "\n";
      }

      // Timing comparison (9 columns)
      if (timing_comparison) {
        timing_comparison << params.numRecords << "," << numCol << "," << result.realDataPercentage << ","
                         << result.avgRealDataMultiTime << "," << result.avgRealDataSingleTime << ","
                         << result.avgFalseDataMultiTime << "," << result.avgFalseDataSingleTime << ","
                         << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << "\n";
      }
    }

    real_data_checks.close();
    real_data_per_column.close();
    scalability_summary.close();
    timing_comparison.close();

    dbManager.closeDB();
  }
}