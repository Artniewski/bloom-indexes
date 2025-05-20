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

void writeExp5CSVHeaders() {
  writeCsvHeader(
      "csv/exp_5_bloom_metrics.csv",
      "numRecords,itemsPerPartition,"
      "globalScanTime_avg,globalScanTime_min,globalScanTime_max,globalScanTime_"
      "median,"
      "hierarchicalSingleTime_avg,hierarchicalSingleTime_min,"
      "hierarchicalSingleTime_max,hierarchicalSingleTime_median,"
      "hierarchicalMultiTime_avg,hierarchicalMultiTime_min,"
      "hierarchicalMultiTime_max,hierarchicalMultiTime_median,"
      "falsePositiveProbability,"
      "multiCol_bloomChecks_avg,multiCol_bloomChecks_min,multiCol_bloomChecks_"
      "max,multiCol_bloomChecks_median,"
      "multiCol_leafBloomChecks_avg,multiCol_leafBloomChecks_min,multiCol_"
      "leafBloomChecks_max,multiCol_leafBloomChecks_median,"
      "multiCol_sstChecks_avg,multiCol_sstChecks_min,multiCol_sstChecks_max,"
      "multiCol_sstChecks_median,"
      "singleCol_bloomChecks_avg,singleCol_bloomChecks_min,singleCol_"
      "bloomChecks_max,singleCol_bloomChecks_median,"
      "singleCol_leafBloomChecks_avg,singleCol_leafBloomChecks_min,singleCol_"
      "leafBloomChecks_max,singleCol_leafBloomChecks_median,"
      "singleCol_sstChecks_avg,singleCol_sstChecks_min,singleCol_sstChecks_max,"
      "singleCol_sstChecks_median");
}

void runExp5(const std::string& dbPath, size_t dbSizeParam) {
  const std::vector<std::string> columns = {"phone", "mail", "address"};
  const size_t bloomFilterSize = 1'000'000;
  const std::vector<size_t> itemsPerPartitionVec = {50000, 100000, 200000};
  const int numQueryRuns = 10;  // Number of times to run queries for statistics

  DBManager dbManager;
  BloomManager bloomManager;

  writeExp5CSVHeaders();

  std::ofstream csv_out("csv/exp_5_bloom_metrics.csv", std::ios::app);
  if (!csv_out) {
    spdlog::error(
        "Exp5: Nie udało się otworzyć pliku wynikowego "
        "csv/exp_5_bloom_metrics.csv do dopisywania!");
    return;
  }

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

    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, columns, dbSizeParam, numQueryRuns);

    double falsePositiveProb = getProbabilityOfFalsePositive(
        params.bloomSize, params.numHashFunctions, params.itemsPerPartition);

    csv_out << params.numRecords << "," << currentItemsPerPartition << ","
            << timings.globalScanTimeStats.average << ","
            << timings.globalScanTimeStats.min << ","
            << timings.globalScanTimeStats.max << ","
            << timings.globalScanTimeStats.median << ","
            << timings.hierarchicalSingleTimeStats.average << ","
            << timings.hierarchicalSingleTimeStats.min << ","
            << timings.hierarchicalSingleTimeStats.max << ","
            << timings.hierarchicalSingleTimeStats.median << ","
            << timings.hierarchicalMultiTimeStats.average << ","
            << timings.hierarchicalMultiTimeStats.min << ","
            << timings.hierarchicalMultiTimeStats.max << ","
            << timings.hierarchicalMultiTimeStats.median << ","
            << falsePositiveProb << ","
            << timings.multiCol_bloomChecksStats.average << ","
            << timings.multiCol_bloomChecksStats.min << ","
            << timings.multiCol_bloomChecksStats.max << ","
            << timings.multiCol_bloomChecksStats.median << ","
            << timings.multiCol_leafBloomChecksStats.average << ","
            << timings.multiCol_leafBloomChecksStats.min << ","
            << timings.multiCol_leafBloomChecksStats.max << ","
            << timings.multiCol_leafBloomChecksStats.median << ","
            << timings.multiCol_sstChecksStats.average << ","
            << timings.multiCol_sstChecksStats.min << ","
            << timings.multiCol_sstChecksStats.max << ","
            << timings.multiCol_sstChecksStats.median << ","
            << timings.singleCol_bloomChecksStats.average << ","
            << timings.singleCol_bloomChecksStats.min << ","
            << timings.singleCol_bloomChecksStats.max << ","
            << timings.singleCol_bloomChecksStats.median << ","
            << timings.singleCol_leafBloomChecksStats.average << ","
            << timings.singleCol_leafBloomChecksStats.min << ","
            << timings.singleCol_leafBloomChecksStats.max << ","
            << timings.singleCol_leafBloomChecksStats.median << ","
            << timings.singleCol_sstChecksStats.average << ","
            << timings.singleCol_sstChecksStats.min << ","
            << timings.singleCol_sstChecksStats.max << ","
            << timings.singleCol_sstChecksStats.median << "\n";
    dbManager.closeDB();
  }
  csv_out.close();
}