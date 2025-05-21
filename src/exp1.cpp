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

    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, columns, dbSize, 10, skipDbScan);

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