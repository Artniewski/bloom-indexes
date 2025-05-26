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

void writeExp6ChecksCSVHeaders() {
  writeCsvHeader(
      "csv/exp_6_checks.csv",
      "numRecords,bloomSize,"
      "multiCol_bloomChecks_avg,multiCol_bloomChecks_min,"
      "multiCol_bloomChecks_max,"
      "multiCol_leafBloomChecks_avg,multiCol_leafBloomChecks_min,"
      "multiCol_leafBloomChecks_max,"
      "multiCol_sstChecks_avg,multiCol_sstChecks_min,multiCol_"
      "sstChecks_max,"
      "singleCol_bloomChecks_avg,singleCol_bloomChecks_min,singleCol_"
      "bloomChecks_max,"
      "singleCol_leafBloomChecks_avg,singleCol_leafBloomChecks_min,"
      "singleCol_leafBloomChecks_max,"
      "singleCol_sstChecks_avg,singleCol_sstChecks_min,singleCol_"
      "sstChecks_max");
}

void writeExp6TimingsCSVHeaders() {
  writeCsvHeader("csv/exp_6_timings.csv",
                 "numRecords,bloomSize,"
                 "hierarchicalSingleTime_avg,hierarchicalSingleTime_min,"
                 "hierarchicalSingleTime_max,"
                 "hierarchicalMultiTime_avg,hierarchicalMultiTime_min,"
                 "hierarchicalMultiTime_max");
}

void writeExp6OverviewCSVHeaders() {
  writeCsvHeader("csv/exp_6_overview.csv",
                 "numRecords,bloomSize,falsePositiveProbability,"
                 "globalScanTime_avg,hierarchicalSingleTime_avg,"
                 "hierarchicalMultiTime_avg");
}

void writeExp6SelectedAvgChecksCSVHeaders() {
  writeCsvHeader("csv/exp_6_selected_avg_checks.csv",
                 "numRec,bloomSize,"
                 "mcBloomAvg,mcLeafAvg,mcSSTAvg,"
                 "scBloomAvg,scLeafAvg,scSSTAvg");
}

void runExp6(const std::string& dbPath, size_t dbSize, bool skipDbScan) {
  const std::vector<std::string> columns = {"phone", "mail", "address"};
  const std::vector<size_t> bloomSizes = {500000, 1000000, 2000000};
  const int numQueryRuns = 10;  // Number of times to run queries for statistics

  DBManager dbManager;
  BloomManager bloomManager;

  writeExp6ChecksCSVHeaders();
  writeExp6TimingsCSVHeaders();
  writeExp6OverviewCSVHeaders();
  writeExp6SelectedAvgChecksCSVHeaders();

  for (const auto& bloomSize : bloomSizes) {
    TestParams params = {
        dbPath, static_cast<int>(dbSize), 3, 1, 100000, bloomSize, 6};
    spdlog::info(
        "Exp6: Rozpoczynam eksperyment dla bazy '{}', rozmiar bloom: {} bits",
        params.dbName, bloomSize);

    clearBloomFilterFiles(params.dbName);
    dbManager.openDB(params.dbName);

    std::map<std::string, BloomTree> hierarchies;

    std::map<std::string, std::vector<std::string>> columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);

    hierarchies = buildHierarchies(columnSstFiles, bloomManager, params);

    // Run standard queries first
    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, columns, dbSize, numQueryRuns, skipDbScan);

    // Then run pattern-based queries
    spdlog::info("Exp6: Running pattern-based queries for {} columns", columns.size());
    std::vector<PatternQueryResult> results = runPatternQueriesWithCsvData(
        dbManager, hierarchies, columns, dbSize);
    
    spdlog::info("Exp6: Generated {} pattern results for {} columns", 
                 results.size(), columns.size());

    double falsePositiveProb = getProbabilityOfFalsePositive(
        params.bloomSize, params.numHashFunctions, params.itemsPerPartition);

    // Zapis wyników do pliku CSV
    std::ofstream checks_csv_out("csv/exp_6_checks.csv", std::ios::app);
    if (!checks_csv_out) {
      spdlog::error(
          "Exp6: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_6_checks.csv do dopisywania!");
      return;
    }
    std::ofstream timings_csv_out("csv/exp_6_timings.csv", std::ios::app);
    if (!timings_csv_out) {
      spdlog::error(
          "Exp6: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_6_timings.csv do dopisywania!");
      return;
    }
    std::ofstream overview_csv_out("csv/exp_6_overview.csv", std::ios::app);
    if (!overview_csv_out) {
      spdlog::error(
          "Exp6: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_6_overview.csv do dopisywania!");
      return;
    }
    std::ofstream selected_avg_checks_csv_out("csv/exp_6_selected_avg_checks.csv", std::ios::app);
    if (!selected_avg_checks_csv_out) {
      spdlog::error(
          "Exp6: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_6_selected_avg_checks.csv do dopisywania!");
      return;
    }

    checks_csv_out << dbSize << "," << bloomSize << ","
                   << timings.multiCol_bloomChecksStats.average << ","
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

    timings_csv_out << dbSize << "," << bloomSize << ","
                    << timings.hierarchicalSingleTimeStats.average << ","
                    << timings.hierarchicalSingleTimeStats.min << ","
                    << timings.hierarchicalSingleTimeStats.max << ","
                    << timings.hierarchicalMultiTimeStats.average << ","
                    << timings.hierarchicalMultiTimeStats.min << ","
                    << timings.hierarchicalMultiTimeStats.max << "\n";

    overview_csv_out << dbSize << "," << bloomSize << "," << falsePositiveProb
                     << "," << timings.globalScanTimeStats.average << ","
                     << timings.hierarchicalSingleTimeStats.average << ","
                     << timings.hierarchicalMultiTimeStats.average << "\n";
    
    selected_avg_checks_csv_out << dbSize << "," << bloomSize << ","
                                << timings.multiCol_bloomChecksStats.average << ","
                                << timings.multiCol_leafBloomChecksStats.average << ","
                                << timings.multiCol_sstChecksStats.average << ","
                                << timings.singleCol_bloomChecksStats.average << ","
                                << timings.singleCol_leafBloomChecksStats.average << ","
                                << timings.singleCol_sstChecksStats.average << "\n";

    // Write pattern-based query results to separate CSV
    std::ofstream pattern_csv("csv/exp_6_false_query.csv", std::ios::app);
    if (!pattern_csv) {
      spdlog::error(
          "Exp6: Nie udało się otworzyć pliku pattern CSV!");
      return;
    }
    
    // Write header if file is empty (first iteration)
    if (bloomSize == bloomSizes[0]) {
      pattern_csv << "NumRecords,BloomSize,PercentageExisting,HierarchicalSingleTime,"
                     "HierarchicalMultiTime,"
                  << "MultiBloomChecks,MultiLeafBloomChecks,MultiSSTChecks,"
                  << "SingleBloomChecks,SingleLeafBloomChecks,SingleSSTChecks\n";
    }
    
    // Write each pattern result as a separate row
    for (const auto& result : results) {
      pattern_csv << dbSize << "," << bloomSize << ","
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

    checks_csv_out.close();
    timings_csv_out.close();
    overview_csv_out.close();
    selected_avg_checks_csv_out.close();
    dbManager.closeDB();
  }
}