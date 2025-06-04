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

void runExp8(std::string baseDir, bool initMode, bool skipDbScan) {
  const int dbSize = 50'000'000;
  const int maxColumns = 12;
  const std::vector<int> numColumnsToTest = {4,8,10,maxColumns};
  
  const std::string fixedDbName = baseDir + "/exp8_shared_db";
  const int numQueriesPerScenario = 100;

  std::vector<std::string> allColumnNames;
  for (int i = 0; i < maxColumns; ++i) {
    allColumnNames.push_back("i_" + std::to_string(i) + "_column");
  }

  DBManager dbManager;
  BloomManager bloomManager;

  // Initialize standard metrics CSV
  std::ofstream out("csv/exp_8_bloom_metrics.csv", std::ios::app);
  if (!out) {
    spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
    return;
  }

  out << "NumRecords,NumColumns,GlobalScanTime,HierarchicalSingleTime,"
         "HierarchicalMultiTime,"
      << "MultiBloomChecks,MultiLeafBloomChecks,MultiSSTChecks,"
      << "SingleBloomChecks,SingleLeafBloomChecks,SingleSSTChecks\n";

  out.close();

  // Initialize derived metrics CSV
  std::ofstream derived_out("csv/exp_8_derived_metrics.csv", std::ios::app);
  if (!derived_out) {
    spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku derived metrics CSV!");
    return;
  }

  derived_out << "NumRecords,NumColumns,GlobalScanTime,HierarchicalSingleTime,"
                 "HierarchicalMultiTime,"
              << "MultiNonLeafBloomChecks,SingleNonLeafBloomChecks\n";

  derived_out.close();

  // Initialize per-column metrics CSV
  std::ofstream per_column_out("csv/exp_8_per_column_metrics.csv", std::ios::app);
  if (!per_column_out) {
    spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku per column metrics CSV!");
    return;
  }

  per_column_out << "NumRecords,NumColumns,GlobalScanTime,HierarchicalSingleTime,"
                    "HierarchicalMultiTime,"
                 << "MultiBloomChecksPerColumn,MultiLeafBloomChecksPerColumn,MultiNonLeafBloomChecksPerColumn,MultiSSTChecksPerColumn,"
                 << "SingleBloomChecksPerColumn,SingleLeafBloomChecksPerColumn,SingleNonLeafBloomChecksPerColumn,SingleSSTChecksPerColumn\n";

  per_column_out.close();

  // Initialize pattern metrics CSV
  std::ofstream pattern_out("csv/exp_8_false_query.csv", std::ios::app);
  if (!pattern_out) {
    spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku pattern CSV!");
    return;
  }

  pattern_out << "NumRecords,NumColumns,PercentageExisting,HierarchicalSingleTime,"
                 "HierarchicalMultiTime,"
              << "MultiBloomChecks,MultiLeafBloomChecks,MultiSSTChecks,"
              << "SingleBloomChecks,SingleLeafBloomChecks,SingleSSTChecks\n";

  pattern_out.close();


  // Initialize comprehensive analysis CSV
  std::ofstream comprehensive_out("csv/exp_8_comprehensive_analysis.csv", std::ios::app);
  if (!comprehensive_out) {
    spdlog::error("ExpBloomMetrics: Nie udało się otworzyć pliku comprehensive analysis CSV!");
    return;
  }

  comprehensive_out << "NumRecords,NumColumns,RealDataPercentage,TotalQueries,RealQueries,FalseQueries,"
                       "AvgHierarchicalMultiTime,AvgHierarchicalSingleTime,"
                       "AvgRealDataMultiTime,AvgRealDataSingleTime,AvgFalseDataMultiTime,AvgFalseDataSingleTime,"
                    << "AvgMultiBloomChecks,AvgMultiLeafBloomChecks,AvgMultiNonLeafBloomChecks,AvgMultiSSTChecks,"
                    << "AvgSingleBloomChecks,AvgSingleLeafBloomChecks,AvgSingleNonLeafBloomChecks,AvgSingleSSTChecks,"
                    << "AvgMultiBloomChecksPerColumn,AvgMultiLeafBloomChecksPerColumn,AvgMultiNonLeafBloomChecksPerColumn,AvgMultiSSTChecksPerColumn,"
                    << "AvgSingleBloomChecksPerColumn,AvgSingleLeafBloomChecksPerColumn,AvgSingleNonLeafBloomChecksPerColumn,AvgSingleSSTChecksPerColumn,"
                    << "AvgRealMultiBloomChecks,AvgRealMultiSSTChecks,AvgFalseMultiBloomChecks,AvgFalseMultiSSTChecks,"
                    << "AvgRealMultiBloomChecksPerColumn,AvgRealMultiSSTChecksPerColumn,AvgFalseMultiBloomChecksPerColumn,AvgFalseMultiSSTChecksPerColumn\n";

  comprehensive_out.close();

  // --- Database Initialization (once for all 10 columns) ---
  spdlog::info(
      "ExpBloomMetrics: Initializing shared database '{}' with {} columns if "
      "it doesn't exist.",
      fixedDbName, maxColumns);
  clearBloomFilterFiles(fixedDbName);  // Clear any old bloom files

  if (std::filesystem::exists(fixedDbName)) {
    spdlog::info(
        "ExpBloomMetrics: Shared database '{}' already exists, skipping "
        "initialization.",
        fixedDbName);
    // Open with all columns to ensure all CFs are recognized
    dbManager.openDB(fixedDbName, allColumnNames);
  } else {
    dbManager.openDB(fixedDbName, allColumnNames);
    dbManager.insertRecords(dbSize, allColumnNames);  // Insert for all columns
    try {
      dbManager.compactAllColumnFamilies(dbSize);
    } catch (const std::exception& e) {
      spdlog::error("Error during initial compaction for '{}': {}", fixedDbName,
                    e.what());
      exit(1);
    }
  }
  // Close DB after initialization; it will be reopened in the loop
  dbManager.closeDB();
  // --- End Database Initialization ---

  for (const auto& numCol : numColumnsToTest) {
    std::vector<std::string> currentColumns;
    for (int i = 0; i < numCol; ++i) {
      currentColumns.push_back(
          allColumnNames[i]);  // Use subset of allColumnNames
    }
    // log columns
    spdlog::info("ExpBloomMetrics: Starting iteration for {} columns:", numCol);
    for (const auto& column : currentColumns) {
      spdlog::info("Using Column: {}", column);
    }

    TestParams params = {fixedDbName,  // Use the fixed DB name
                         dbSize,      3, 1, 100000, 1'000'000, 6};
    // The dbName in params is now the fixedDbName, logging should reflect this.
    spdlog::info(
        "ExpBloomMetrics: Running experiment for database '{}' using {}/{} "
        "columns",
        params.dbName, numCol, maxColumns);

    // No need to clear bloom filter files here again if done per experiment
    // setup
    clearBloomFilterFiles(params.dbName);

    // Open the DB with ALL column families that exist in the database.
    // Subsequent operations will use `currentColumns` to operate on a subset.
    dbManager.openDB(params.dbName, allColumnNames);

    // scanSstFilesAsync, buildHierarchies, and runStandardQueries
    // should internally use the 'currentColumns' to restrict their operations.
    std::map<std::string, std::vector<std::string>> columnSstFiles =
        scanSstFilesAsync(currentColumns, dbManager, params);

    std::map<std::string, BloomTree> hierarchies =
        buildHierarchies(columnSstFiles, bloomManager, params);

    // Run standard queries first
    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, currentColumns, dbSize, 100, skipDbScan);

    // Then run pattern-based queries
    spdlog::info("ExpBloomMetrics: Running pattern-based queries for {} columns", numCol);
    std::vector<PatternQueryResult> results = runPatternQueriesWithCsvData(
        dbManager, hierarchies, currentColumns, dbSize);
    
    spdlog::info("ExpBloomMetrics: Generated {} pattern results for {} columns", 
                 results.size(), numCol);

    
    spdlog::info("ExpBloomMetrics: Running comprehensive analysis for {} columns with {} queries per scenario", 
                 numCol, numQueriesPerScenario);
    std::vector<AccumulatedQueryMetrics> comprehensiveResults = runComprehensiveQueryAnalysis(
        dbManager, hierarchies, currentColumns, dbSize, numQueriesPerScenario);
    
    spdlog::info("ExpBloomMetrics: Generated {} comprehensive analysis results for {} columns", 
                 comprehensiveResults.size(), numCol);

    // Write standard query results to original CSV
    std::ofstream out_csv("csv/exp_8_bloom_metrics.csv", std::ios::app);
    if (!out_csv) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
      return;
    }
    
    // Write derived metrics results to separate CSV
    std::ofstream derived_csv("csv/exp_8_derived_metrics.csv", std::ios::app);
    if (!derived_csv) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku derived metrics CSV!");
      return;
    }
    
    // Write per-column metrics results to separate CSV  
    std::ofstream per_column_csv("csv/exp_8_per_column_metrics.csv", std::ios::app);
    if (!per_column_csv) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku per column metrics CSV!");
      return;
    }
    
    out_csv << params.numRecords << "," << numCol << ","
            << timings.globalScanTimeStats.average << ","
            << timings.hierarchicalSingleTimeStats.average << ","
            << timings.hierarchicalMultiTimeStats.average << ","
            << timings.multiCol_bloomChecksStats.average << ","
            << timings.multiCol_leafBloomChecksStats.average << ","
            << timings.multiCol_sstChecksStats.average << ","
            << timings.singleCol_bloomChecksStats.average << ","
            << timings.singleCol_leafBloomChecksStats.average << ","
            << timings.singleCol_sstChecksStats.average << "\n";
    out_csv.close();
    
    derived_csv << params.numRecords << "," << numCol << ","
                << timings.globalScanTimeStats.average << ","
                << timings.hierarchicalSingleTimeStats.average << ","
                << timings.hierarchicalMultiTimeStats.average << ","
                << timings.multiCol_nonLeafBloomChecksStats.average << ","
                << timings.singleCol_nonLeafBloomChecksStats.average << "\n";
    derived_csv.close();
    
    per_column_csv << params.numRecords << "," << numCol << ","
                   << timings.globalScanTimeStats.average << ","
                   << timings.hierarchicalSingleTimeStats.average << ","
                   << timings.hierarchicalMultiTimeStats.average << ","
                   << timings.multiCol_bloomChecksPerColumnStats.average << ","
                   << timings.multiCol_leafBloomChecksPerColumnStats.average << ","
                   << timings.multiCol_nonLeafBloomChecksPerColumnStats.average << ","
                   << timings.multiCol_sstChecksPerColumnStats.average << ","
                   << timings.singleCol_bloomChecksPerColumnStats.average << ","
                   << timings.singleCol_leafBloomChecksPerColumnStats.average << ","
                   << timings.singleCol_nonLeafBloomChecksPerColumnStats.average << ","
                   << timings.singleCol_sstChecksPerColumnStats.average << "\n";
    per_column_csv.close();

    // Write pattern-based query results to separate CSV
    std::ofstream pattern_csv("csv/exp_8_false_query.csv", std::ios::app);
    if (!pattern_csv) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku pattern CSV!");
      return;
    }
    
    // Write each pattern result as a separate row
    for (const auto& result : results) {
      pattern_csv << params.numRecords << "," << numCol << ","
                  << result.percent << ","
                  << result.hierarchicalSingleTime << ","
                  << result.hierarchicalMultiTime << ","
                  << result.multiCol_bloomChecks << ","
                  << result.multiCol_leafBloomChecks << ","
                  << result.multiCol_sstChecks << ","
                  << result.singleCol_bloomChecks << ","
                  << result.singleCol_leafBloomChecks << ","
                  << result.singleCol_sstChecks << "\n";
    }
    pattern_csv.close();


    // Write comprehensive analysis results to separate CSV
    std::ofstream comprehensive_csv("csv/exp_8_comprehensive_analysis.csv", std::ios::app);
    if (!comprehensive_csv) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku comprehensive analysis CSV!");
      return;
    }
    
    // Write each comprehensive analysis result as a separate row
    for (const auto& result : comprehensiveResults) {
      comprehensive_csv << params.numRecords << "," << numCol << ","
                        << result.realDataPercentage << "," << result.totalQueries << ","
                        << result.realQueries << "," << result.falseQueries << ","
                        << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << ","
                        << result.avgRealDataMultiTime << "," << result.avgRealDataSingleTime << ","
                        << result.avgFalseDataMultiTime << "," << result.avgFalseDataSingleTime << ","
                        << result.avgMultiBloomChecks << "," << result.avgMultiLeafBloomChecks << ","
                        << result.avgMultiNonLeafBloomChecks << "," << result.avgMultiSSTChecks << ","
                        << result.avgSingleBloomChecks << "," << result.avgSingleLeafBloomChecks << ","
                        << result.avgSingleNonLeafBloomChecks << "," << result.avgSingleSSTChecks << ","
                        << result.avgMultiBloomChecksPerColumn << "," << result.avgMultiLeafBloomChecksPerColumn << ","
                        << result.avgMultiNonLeafBloomChecksPerColumn << "," << result.avgMultiSSTChecksPerColumn << ","
                        << result.avgSingleBloomChecksPerColumn << "," << result.avgSingleLeafBloomChecksPerColumn << ","
                        << result.avgSingleNonLeafBloomChecksPerColumn << "," << result.avgSingleSSTChecksPerColumn << ","
                        << result.avgRealMultiBloomChecks << "," << result.avgRealMultiSSTChecks << ","
                        << result.avgFalseMultiBloomChecks << "," << result.avgFalseMultiSSTChecks << ","
                        << result.avgRealMultiBloomChecksPerColumn << "," << result.avgRealMultiSSTChecksPerColumn << ","
                        << result.avgFalseMultiBloomChecksPerColumn << "," << result.avgFalseMultiSSTChecksPerColumn << "\n";
    }
    comprehensive_csv.close();

    dbManager.closeDB();
  }
}