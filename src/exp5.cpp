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

void writeExp5ChecksCSVHeaders() {
  writeCsvHeader(
      "csv/exp_5_checks.csv",
      "numRecords,itemsPerPartition,"
      "multiCol_bloomChecks_avg,multiCol_bloomChecks_min,"
      "multiCol_bloomChecks_max,"
      "multiCol_leafBloomChecks_avg,multiCol_leafBloomChecks_min,"
      "multiCol_leafBloomChecks_max,"
      "multiCol_sstChecks_avg,multiCol_sstChecks_min,multiCol_"
      "sstChecks_max,"
      "singleCol_bloomChecks_avg,singleCol_bloomChecks_min,singleCol_"
      "bloomChecks_max,"
      "singleCol_leafBlo omChecks_avg,singleCol_leafBloomChecks_min,"
      "singleCol_leafBloomChecks_max,"
      "singleCol_sstChecks_avg,singleCol_sstChecks_min,singleCol_"
      "sstChecks_max");
}

void writeExp5TimingsCSVHeaders() {
  writeCsvHeader("csv/exp_5_timings.csv",
                 "numRecords,itemsPerPartition,"
                 "hierarchicalSingleTime_avg,hierarchicalSingleTime_min,"
                 "hierarchicalSingleTime_max,"
                 "hierarchicalMultiTime_avg,hierarchicalMultiTime_min,"
                 "hierarchicalMultiTime_max");
}

void writeExp5OverviewCSVHeaders() {
  writeCsvHeader("csv/exp_5_overview.csv",
                 "numRecords,itemsPerPartition,falsePositiveProbability,"
                 "globalScanTime_avg,hierarchicalSingleTime_avg,"
                 "hierarchicalMultiTime_avg");
}

void writeExp5SelectedAvgChecksCSVHeaders() {
  writeCsvHeader("csv/exp_5_selected_avg_checks.csv",
                 "numRec,itemsPart,"
                 "mcBloomAvg,mcLeafAvg,mcSSTAvg,"
                 "scBloomAvg,scLeafAvg,scSSTAvg");
}

void runExp5(const std::string& dbPath, size_t dbSizeParam, bool skipDbScan) {
  const std::vector<std::string> columns = {"phone", "mail", "address"};
  const size_t bloomFilterSize = 1'000'000;
  const std::vector<size_t> itemsPerPartitionVec = {50000, 75000, 100000};
  const int numQueryRuns = 10;  // Number of times to run queries for statistics

  DBManager dbManager;
  BloomManager bloomManager;

  writeExp5ChecksCSVHeaders();
  writeExp5TimingsCSVHeaders();
  writeExp5OverviewCSVHeaders();
  writeExp5SelectedAvgChecksCSVHeaders();

  for (const auto& currentItemsPerPartition : itemsPerPartitionVec) {
    TestParams params = {dbPath, static_cast<int>(dbSizeParam), 3,
                         1,      currentItemsPerPartition,      bloomFilterSize,
                         6};
    spdlog::info("Exp5: Running for DB: '{}', itemsPerPartition: {}",
                 params.dbName, currentItemsPerPartition);

    clearBloomFilterFiles(params.dbName);
    dbManager.openDB(params.dbName);

    std::map<std::string, BloomTree> hierarchies;

    std::map<std::string, std::vector<std::string>> columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);

    hierarchies = buildHierarchies(columnSstFiles, bloomManager, params);

    // Run standard queries first
    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, columns, dbSizeParam, numQueryRuns, skipDbScan);

    // Then run pattern-based queries
    spdlog::info("Exp5: Running pattern-based queries for {} columns", columns.size());
    std::vector<PatternQueryResult> results = runPatternQueriesWithCsvData(
        dbManager, hierarchies, columns, dbSizeParam);
    
    spdlog::info("Exp5: Generated {} pattern results for {} columns", 
                 results.size(), columns.size());

    // Run comprehensive analysis across different real data percentages
    const int numQueriesPerScenario = 10;  // Number of queries per percentage scenario
    
    spdlog::info("Exp5: Running comprehensive analysis for {} columns with {} queries per scenario", 
                 columns.size(), numQueriesPerScenario);
    std::vector<AccumulatedQueryMetrics> comprehensiveResults = runComprehensiveQueryAnalysis(
        dbManager, hierarchies, columns, dbSizeParam, numQueriesPerScenario);
    
    spdlog::info("Exp5: Generated {} comprehensive analysis results for {} columns", 
                 comprehensiveResults.size(), columns.size());

    double falsePositiveProb = getProbabilityOfFalsePositive(
        params.bloomSize, params.numHashFunctions, params.itemsPerPartition);

    size_t totalDiskBloomSize = 0;
    size_t totalMemoryBloomSize = 0;
    for (const auto& kv : hierarchies) {
      const BloomTree& tree = kv.second;
      totalDiskBloomSize += tree.diskSize();
      totalMemoryBloomSize += tree.memorySize();
    }
    int leafs = hierarchies.at(columns[0]).leafNodes.size();

    writeCsvHeader("csv/exp_2_bloom_metrics.csv",
                   "dbSize,items,fpp,leafs,diskBloomSize,memoryBloomSize");
    std::ofstream bloom_metrics_2_csv_out("csv/exp_2_bloom_metrics.csv",
                                          std::ios::app);
    if (!bloom_metrics_2_csv_out) {
      spdlog::error(
          "Exp5: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_2_bloom_metrics.csv do dopisywania!");
      return;
    }
    std::ofstream checks_csv_out("csv/exp_5_checks.csv", std::ios::app);
    if (!checks_csv_out) {
      spdlog::error(
          "Exp5: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_5_checks.csv do dopisywania!");
      return;
    }
    std::ofstream timings_csv_out("csv/exp_5_timings.csv", std::ios::app);
    if (!timings_csv_out) {
      spdlog::error(
          "Exp5: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_5_timings.csv do dopisywania!");
      return;
    }
    std::ofstream overview_csv_out("csv/exp_5_overview.csv", std::ios::app);
    if (!overview_csv_out) {
      spdlog::error(
          "Exp5: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_5_overview.csv do dopisywania!");
      return;
    }
    std::ofstream selected_avg_checks_csv_out(
        "csv/exp_5_selected_avg_checks.csv", std::ios::app);
    if (!selected_avg_checks_csv_out) {
      spdlog::error(
          "Exp5: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_5_selected_avg_checks.csv do dopisywania!");
      return;
    }
    bloom_metrics_2_csv_out
        << params.numRecords << "," << currentItemsPerPartition << ","
        << falsePositiveProb << "," << leafs << "," << totalDiskBloomSize << ","
        << totalMemoryBloomSize << "\n";

    checks_csv_out << params.numRecords << "," << currentItemsPerPartition
                   << "," << timings.multiCol_bloomChecksStats.average << ","
                   << timings.multiCol_bloomChecksStats.min << ","
                   << timings.multiCol_bloomChecksStats.max << ","
                   << timings.multiCol_leafBloomChecksStats.average << ","
                   << timings.multiCol_leafBloomChecksStats.min << ","
                   << timings.multiCol_leafBloomChecksStats.max << ","
                   << timings.multiCol_sstChecksStats.average << ","
                   << timings.multiCol_sstChecksStats.min << ","
                   << timings.multiCol_sstChecksStats.max << ","
                   << timings.singleCol_bloomChecksStats.average << ","
                   << timings.singleCol_bloomChecksStats.min << ","
                   << timings.singleCol_bloomChecksStats.max << ","
                   << timings.singleCol_leafBloomChecksStats.average << ","
                   << timings.singleCol_leafBloomChecksStats.min << ","
                   << timings.singleCol_leafBloomChecksStats.max << ","
                   << timings.singleCol_sstChecksStats.average << ","
                   << timings.singleCol_sstChecksStats.min << ","
                   << timings.singleCol_sstChecksStats.max << "\n";

    timings_csv_out << params.numRecords << "," << currentItemsPerPartition
                    << "," << timings.hierarchicalSingleTimeStats.average << ","
                    << timings.hierarchicalSingleTimeStats.min << ","
                    << timings.hierarchicalSingleTimeStats.max << ","
                    << timings.hierarchicalMultiTimeStats.average << ","
                    << timings.hierarchicalMultiTimeStats.min << ","
                    << timings.hierarchicalMultiTimeStats.max << "\n";

    overview_csv_out << params.numRecords << "," << currentItemsPerPartition
                     << "," << falsePositiveProb << ","
                     << timings.globalScanTimeStats.average << ","
                     << timings.hierarchicalSingleTimeStats.average << ","
                     << timings.hierarchicalMultiTimeStats.average << "\n";

    selected_avg_checks_csv_out
        << params.numRecords << "," << currentItemsPerPartition << ","
        << timings.multiCol_bloomChecksStats.average << ","
        << timings.multiCol_leafBloomChecksStats.average << ","
        << timings.multiCol_sstChecksStats.average << ","
        << timings.singleCol_bloomChecksStats.average << ","
        << timings.singleCol_leafBloomChecksStats.average << ","
        << timings.singleCol_sstChecksStats.average << "\n";

    // Write pattern-based query results to separate CSV
    std::ofstream pattern_csv("csv/exp_5_false_query.csv", std::ios::app);
    if (!pattern_csv) {
      spdlog::error(
          "Exp5: Nie udało się otworzyć pliku pattern CSV!");
      return;
    }
    
    // Write header if file is empty (first iteration)
    if (currentItemsPerPartition == itemsPerPartitionVec[0]) {
      pattern_csv << "NumRecords,ItemsPerPartition,PercentageExisting,HierarchicalSingleTime,"
                     "HierarchicalMultiTime,"
                  << "MultiBloomChecks,MultiLeafBloomChecks,MultiSSTChecks,"
                  << "SingleBloomChecks,SingleLeafBloomChecks,SingleSSTChecks\n";
    }
    
    // Write each pattern result as a separate row
    for (const auto& result : results) {
      pattern_csv << params.numRecords << "," << currentItemsPerPartition << ","
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
    std::ofstream comprehensive_csv("csv/exp_5_comprehensive_analysis.csv", std::ios::app);
    if (!comprehensive_csv) {
      spdlog::error(
          "Exp5: Nie udało się otworzyć pliku comprehensive analysis CSV!");
      return;
    }
    
    // Write header if file is empty (first iteration)
    if (currentItemsPerPartition == itemsPerPartitionVec[0]) {
      comprehensive_csv << "NumRecords,ItemsPerPartition,RealDataPercentage,TotalQueries,RealQueries,FalseQueries,"
                           "AvgHierarchicalMultiTime,AvgHierarchicalSingleTime,"
                           "AvgRealDataMultiTime,AvgRealDataSingleTime,AvgFalseDataMultiTime,AvgFalseDataSingleTime,"
                        << "AvgMultiBloomChecks,AvgMultiLeafBloomChecks,AvgMultiSSTChecks,"
                        << "AvgSingleBloomChecks,AvgSingleLeafBloomChecks,AvgSingleSSTChecks,"
                        << "AvgRealMultiBloomChecks,AvgRealMultiSSTChecks,AvgFalseMultiBloomChecks,AvgFalseMultiSSTChecks\n";
    }
    
    // Write each comprehensive analysis result as a separate row
    for (const auto& result : comprehensiveResults) {
      comprehensive_csv << params.numRecords << "," << currentItemsPerPartition << ","
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
    bloom_metrics_2_csv_out.close();
    checks_csv_out.close();
    timings_csv_out.close();
    overview_csv_out.close();
    selected_avg_checks_csv_out.close();
  }
}