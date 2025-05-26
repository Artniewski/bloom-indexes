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

void runExp1(std::string baseDir, bool initMode, std::string sharedDbName,
             int defaultNumRecords, bool skipDbScan) {
  writeCsvHeaders();

  const std::vector<std::string> columns = {"phone", "mail", "address"};
  const std::vector<int> dbSizes = {10'000'000, 20'000'000, 50'000'000};

  DBManager dbManager;
  BloomManager bloomManager;
  StopWatch stopwatch;

  for (const auto& dbSize : dbSizes) {
    std::string dbName = (dbSize == defaultNumRecords)
                             ? sharedDbName
                             : baseDir + "/exp1_db_" + std::to_string(dbSize);
    TestParams params = {dbName, dbSize, 3, 1, 100000, 1'000'000, 6};
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

    // Then run pattern-based queries
    spdlog::info("ExpBloomMetrics: Running pattern-based queries for {} columns", columns.size());
    std::vector<PatternQueryResult> results = runPatternQueriesWithCsvData(
        dbManager, hierarchies, columns, dbSize);
    
    spdlog::info("ExpBloomMetrics: Generated {} pattern results for {} columns", 
                 results.size(), columns.size());

    // Run comprehensive analysis across different real data percentages
    const int numQueriesPerScenario = 100;  // Number of queries per percentage scenario
    
    spdlog::info("ExpBloomMetrics: Running comprehensive analysis for {} columns with {} queries per scenario", 
                 columns.size(), numQueriesPerScenario);
    std::vector<AccumulatedQueryMetrics> comprehensiveResults = runComprehensiveQueryAnalysis(
        dbManager, hierarchies, columns, dbSize, numQueriesPerScenario);
    
    spdlog::info("ExpBloomMetrics: Generated {} comprehensive analysis results for {} columns", 
                 comprehensiveResults.size(), columns.size());

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

    // Write pattern-based query results to separate CSV
    std::ofstream pattern_csv("csv/exp_1_false_query.csv", std::ios::app);
    if (!pattern_csv) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku pattern CSV!");
      return;
    }
    
    if (dbSize == dbSizes[0]) {
      pattern_csv << "NumRecords,NumColumns,PercentageExisting,HierarchicalSingleTime,"
                     "HierarchicalMultiTime,"
                  << "MultiBloomChecks,MultiLeafBloomChecks,MultiSSTChecks,"
                  << "SingleBloomChecks,SingleLeafBloomChecks,SingleSSTChecks\n";
    }
    
    for (const auto& result : results) {
      pattern_csv << dbSize << "," << columns.size() << ","
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
    std::ofstream comprehensive_csv("csv/exp_1_comprehensive_analysis.csv", std::ios::app);
    if (!comprehensive_csv) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku comprehensive analysis CSV!");
      return;
    }
    
    // Write header if file is empty (first iteration)
    if (dbSize == dbSizes[0]) {
      comprehensive_csv << "NumRecords,RealDataPercentage,TotalQueries,RealQueries,FalseQueries,"
                           "AvgHierarchicalMultiTime,AvgHierarchicalSingleTime,"
                           "AvgRealDataMultiTime,AvgRealDataSingleTime,AvgFalseDataMultiTime,AvgFalseDataSingleTime,"
                        << "AvgMultiBloomChecks,AvgMultiLeafBloomChecks,AvgMultiSSTChecks,"
                        << "AvgSingleBloomChecks,AvgSingleLeafBloomChecks,AvgSingleSSTChecks,"
                        << "AvgRealMultiBloomChecks,AvgRealMultiSSTChecks,AvgFalseMultiBloomChecks,AvgFalseMultiSSTChecks\n";
    }
    
    // Write each comprehensive analysis result as a separate row
    for (const auto& result : comprehensiveResults) {
      comprehensive_csv << dbSize << ","
                        << result.realDataPercentage << "," << result.totalQueries << ","
                        << result.realQueries << "," << result.falseQueries << ","
                        << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << ","
                        << result.avgRealDataMultiTime << "," << result.avgRealDataSingleTime << ","
                        << result.avgFalseDataMultiTime << "," << result.avgFalseDataSingleTime << ","
                        << result.avgMultiBloomChecks << "," << result.avgMultiLeafBloomChecks << ","
                        << result.avgMultiSSTChecks << "," << result.avgSingleBloomChecks << ","
                        << result.avgSingleLeafBloomChecks << "," << result.avgSingleSSTChecks << ","
                        << result.avgRealMultiBloomChecks << "," << result.avgRealMultiSSTChecks << ","
                        << result.avgFalseMultiBloomChecks << "," << result.avgFalseMultiSSTChecks << "\n";
    }
    comprehensive_csv.close();

    dbManager.closeDB();
  }
}

void writeCsvHeaders() {
  writeCsvHeader(
      "csv/exp_1_bloom_metrics.csv",
      "numRecords,bloomTreeRatio,itemsPerPartition,bloomSize,numHashFunctions,"
      "singleHierarchyLeafs,bloomDiskSize,blomMemSize");
}