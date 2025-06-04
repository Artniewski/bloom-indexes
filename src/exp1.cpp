#include "exp1.hpp"

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

void writeCsvExp3Headers() {
  writeCsvHeader("csv/exp_3_bloom_metrics.csv",
                 "numRecords,bloomCreationTime,dbCreationTime");
}

void writeExp1BasicMetricsHeaders() {
  writeCsvHeader("csv/exp_1_basic_metrics.csv",
                 "dbSize,globalScanTime,hierarchicalSingleTime,hierarchicalMultiTime");
}

void writeExp1BasicChecksHeaders() {
  writeCsvHeader("csv/exp_1_basic_checks.csv",
                 "dbSize,multiBloomChecks,multiLeafBloomChecks,multiSSTChecks,"
                 "singleBloomChecks,singleLeafBloomChecks,singleSSTChecks");
}

void writeExp1PatternTimingsHeaders() {
  writeCsvHeader("csv/exp_1_pattern_timings.csv",
                 "dbSize,percentageExisting,hierarchicalSingleTime,hierarchicalMultiTime");
}

void writeExp1ComprehensiveChecksHeaders() {
  writeCsvHeader("csv/exp_1_comprehensive_checks.csv",
                 "dbSize,realDataPercentage,"
                 "avgMultiBloomChecks,avgMultiLeafBloomChecks,avgMultiNonLeafBloomChecks,avgMultiSSTChecks,"
                 "avgSingleBloomChecks,avgSingleLeafBloomChecks,avgSingleNonLeafBloomChecks,avgSingleSSTChecks,"
                 "avgRealMultiBloomChecks,avgRealMultiSSTChecks,avgFalseMultiBloomChecks,avgFalseMultiSSTChecks");
}

void writeExp1PerColumnHeaders() {
  writeCsvHeader("csv/exp_1_per_column_stats.csv",
                 "dbSize,realDataPercentage,numColumns,"
                 "avgMultiBloomPerCol,avgMultiLeafPerCol,avgMultiNonLeafPerCol,avgMultiSSTPerCol,"
                 "avgSingleBloomPerCol,avgSingleLeafPerCol,avgSingleNonLeafPerCol,avgSingleSSTPerCol,"
                 "avgRealMultiBloomPerCol,avgRealMultiSSTPerCol,avgFalseMultiBloomPerCol,avgFalseMultiSSTPerCol");
}

void writeExp1MixedQueryHeaders() {
  writeCsvHeader("csv/exp_1_mixed_query_summary.csv",
                 "dbSize,realDataPercentage,totalQueries,realQueries,falseQueries,"
                 "avgMultiTime,avgSingleTime,avgMultiBloomChecks,avgMultiSSTChecks");
}

void writeExp1TimingComparisonHeaders() {
  writeCsvHeader("csv/exp_1_timing_comparison.csv",
                 "dbSize,realDataPercentage,"
                 "avgRealMultiTime,avgRealSingleTime,"
                 "avgFalseMultiTime,avgFalseSingleTime");
}

void runExp1(std::string baseDir, bool initMode, std::string sharedDbName,
             int defaultNumRecords, bool skipDbScan) {
  writeCsvHeaders();
  writeExp1BasicMetricsHeaders();
  writeExp1BasicChecksHeaders();
  writeExp1PatternTimingsHeaders();
  writeExp1ComprehensiveChecksHeaders();
  writeExp1PerColumnHeaders();
  writeExp1MixedQueryHeaders();
  writeExp1TimingComparisonHeaders();

  const std::vector<std::string> columns = {"phone", "mail", "address"};
  const std::vector<int> dbSizes = {10'000'000, 15'000'000, defaultNumRecords};

  DBManager dbManager;
  BloomManager bloomManager;
  StopWatch stopwatch;

  for (const auto& dbSize : dbSizes) {
    std::string dbName = (dbSize == defaultNumRecords)
                             ? sharedDbName
                             : baseDir + "/exp1_db_" + std::to_string(dbSize);
    TestParams params = {dbName, dbSize, 3, 1, 100000, 4'000'000, 3};
    spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'",
                 params.dbName);
    clearBloomFilterFiles(params.dbName);

    stopwatch.start();
    if (std::filesystem::exists(params.dbName)) {
      spdlog::info(
          "EXP1: Database '{}' already exists, skipping initialization.",
          params.dbName);
      dbManager.openDB(params.dbName, columns);
    } else {
      dbManager.openDB(params.dbName, columns);
      dbManager.insertRecords(params.numRecords, columns);
      try {
        dbManager.compactAllColumnFamilies(params.numRecords);
      } catch (const std::exception& e) {
        spdlog::error("Error '{}'", e.what());
        exit(1);
      }
    }
    stopwatch.stop();
    auto dbCreationTime = stopwatch.elapsedMicros();

    stopwatch.start();
    std::map<std::string, std::vector<std::string>> columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);
    std::map<std::string, BloomTree> hierarchies;
    hierarchies = buildHierarchies(columnSstFiles, bloomManager, params);
    stopwatch.stop();
    auto bloomCreationTime = stopwatch.elapsedMicros();

    size_t totalDiskBloomSize = 0;
    size_t totalMemoryBloomSize = 0;
    for (const auto& kv : hierarchies) {
      const BloomTree& tree = kv.second;
      totalDiskBloomSize += tree.diskSize();
      totalMemoryBloomSize += tree.memorySize();
    }

    std::ofstream out("csv/exp_1_bloom_metrics.csv", std::ios::app);
    if (!out) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
      return;
    }
    out << params.numRecords << "," << params.bloomTreeRatio << ","
        << params.itemsPerPartition << "," << params.bloomSize << ","
        << params.numHashFunctions << ","
        << hierarchies.at(columns[0]).leafNodes.size() << ","
        << totalDiskBloomSize << "," << totalMemoryBloomSize << "\n";
    out.close();
    spdlog::info("ExpBloomMetrics: Eksperyment dla bazy '{}' zakończony.",
                 params.dbName);
    writeCsvExp3Headers();
    std::ofstream outExp3("csv/exp_3_bloom_metrics.csv", std::ios::app);
    if (!outExp3) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
      return;
    }
    outExp3 << params.numRecords << "," << dbCreationTime << ","
            << bloomCreationTime << "\n";
    outExp3.close();

    // Run standard queries first
    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, columns, dbSize, 10, skipDbScan);

    // Write basic metrics - timings only (4 columns)
    std::ofstream basic_metrics("csv/exp_1_basic_metrics.csv", std::ios::app);
    if (basic_metrics) {
      basic_metrics << dbSize << ","
                    << timings.globalScanTimeStats.average << ","
                    << timings.hierarchicalSingleTimeStats.average << ","
                    << timings.hierarchicalMultiTimeStats.average << "\n";
      basic_metrics.close();
    }

    // Write basic checks - check counts only (6 columns)
    std::ofstream basic_checks("csv/exp_1_basic_checks.csv", std::ios::app);
    if (basic_checks) {
      basic_checks << dbSize << ","
                   << timings.multiCol_bloomChecksStats.average << ","
                   << timings.multiCol_leafBloomChecksStats.average << ","
                   << timings.multiCol_sstChecksStats.average << ","
                   << timings.singleCol_bloomChecksStats.average << ","
                   << timings.singleCol_leafBloomChecksStats.average << ","
                   << timings.singleCol_sstChecksStats.average << "\n";
      basic_checks.close();
    }

    // Then run pattern-based queries
    spdlog::info("ExpBloomMetrics: Running pattern-based queries for {} columns", columns.size());
    std::vector<PatternQueryResult> results = runPatternQueriesWithCsvData(
        dbManager, hierarchies, columns, dbSize);
    
    spdlog::info("ExpBloomMetrics: Generated {} pattern results for {} columns", 
                 results.size(), columns.size());

    // Write pattern results to focused CSV files
    std::ofstream pattern_timings("csv/exp_1_pattern_timings.csv", std::ios::app);

    for (const auto& result : results) {
      // Pattern timings (4 columns)
      if (pattern_timings) {
        pattern_timings << dbSize << "," << result.percent << ","
                       << result.hierarchicalSingleTime << ","
                       << result.hierarchicalMultiTime << "\n";
      }
    }
    pattern_timings.close();

    // Run comprehensive analysis across different real data percentages
    const int numQueriesPerScenario = 100;
    
    spdlog::info("ExpBloomMetrics: Running comprehensive analysis for {} columns with {} queries per scenario", 
                 columns.size(), numQueriesPerScenario);
    std::vector<AccumulatedQueryMetrics> comprehensiveResults = runComprehensiveQueryAnalysis(
        dbManager, hierarchies, columns, dbSize, numQueriesPerScenario);
    
    spdlog::info("ExpBloomMetrics: Generated {} comprehensive analysis results for {} columns", 
                 comprehensiveResults.size(), columns.size());

    // Write comprehensive analysis to focused CSV files
    std::ofstream mixed_query_summary("csv/exp_1_mixed_query_summary.csv", std::ios::app);
    std::ofstream timing_comparison("csv/exp_1_timing_comparison.csv", std::ios::app);
    std::ofstream comprehensive_checks("csv/exp_1_comprehensive_checks.csv", std::ios::app);
    std::ofstream per_column_stats("csv/exp_1_per_column_stats.csv", std::ios::app);

    for (const auto& result : comprehensiveResults) {
      // Mixed query summary (8 columns)
      if (mixed_query_summary) {
        mixed_query_summary << dbSize << ","
                           << result.realDataPercentage << "," << result.totalQueries << ","
                           << result.realQueries << "," << result.falseQueries << ","
                           << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << ","
                           << result.avgMultiBloomChecks << "," << result.avgMultiSSTChecks << "\n";
      }

      // Timing comparison (6 columns)
      if (timing_comparison) {
        timing_comparison << dbSize << "," << result.realDataPercentage << ","
                         << result.avgRealDataMultiTime << "," << result.avgRealDataSingleTime << ","
                         << result.avgFalseDataMultiTime << "," << result.avgFalseDataSingleTime << "\n";
      }

      // Comprehensive checks (14 columns)
      if (comprehensive_checks) {
        comprehensive_checks << dbSize << "," << result.realDataPercentage << ","
                            << result.avgMultiBloomChecks << "," << result.avgMultiLeafBloomChecks << ","
                            << result.avgMultiNonLeafBloomChecks << "," << result.avgMultiSSTChecks << ","
                            << result.avgSingleBloomChecks << "," << result.avgSingleLeafBloomChecks << ","
                            << result.avgSingleNonLeafBloomChecks << "," << result.avgSingleSSTChecks << ","
                            << result.avgRealMultiBloomChecks << "," << result.avgRealMultiSSTChecks << ","
                            << result.avgFalseMultiBloomChecks << "," << result.avgFalseMultiSSTChecks << "\n";
      }

      // Per-column stats (14 columns)
      if (per_column_stats) {
        per_column_stats << dbSize << "," << result.realDataPercentage << "," << result.numColumns << ","
                        << result.avgMultiBloomChecksPerColumn << "," << result.avgMultiLeafBloomChecksPerColumn << ","
                        << result.avgMultiNonLeafBloomChecksPerColumn << "," << result.avgMultiSSTChecksPerColumn << ","
                        << result.avgSingleBloomChecksPerColumn << "," << result.avgSingleLeafBloomChecksPerColumn << ","
                        << result.avgSingleNonLeafBloomChecksPerColumn << "," << result.avgSingleSSTChecksPerColumn << ","
                        << result.avgRealMultiBloomChecksPerColumn << "," << result.avgRealMultiSSTChecksPerColumn << ","
                        << result.avgFalseMultiBloomChecksPerColumn << "," << result.avgFalseMultiSSTChecksPerColumn << "\n";
      }
    }
    mixed_query_summary.close();
    timing_comparison.close();
    comprehensive_checks.close();
    per_column_stats.close();

    // Keep exp_4_query_timings.csv for backwards compatibility
    std::ofstream outExp4("csv/exp_4_query_timings.csv", std::ios::app);
    if (!outExp4) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
      return;
    }
    outExp4 << "dbSize,globalScanTime,hierarchicalMultiColumnTime,"
           "hierarchicalSingleColumnTime\n";
    outExp4 << dbSize << "," << timings.globalScanTimeStats.average << ","
        << timings.hierarchicalMultiTimeStats.average << ","
        << timings.hierarchicalSingleTimeStats.average << "\n";
    outExp4.close();

    dbManager.closeDB();
  }
}

void writeCsvHeaders() {
  writeCsvHeader(
      "csv/exp_1_bloom_metrics.csv",
      "numRecords,bloomTreeRatio,itemsPerPartition,bloomSize,numHashFunctions,"
      "singleHierarchyLeafs,bloomDiskSize,blomMemSize");
}