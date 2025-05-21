#include <spdlog/spdlog.h>

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
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

void runExp3(std::string baseDir, bool initMode, std::string sharedDbName,
             int defaultNumRecords) {
  const std::vector<std::string> columns = {"phone", "mail", "address"};
  const std::vector<int> dbSizes = {10'000'000, 20'000'000, 50'000'000};

  DBManager dbManager;
  BloomManager bloomManager;

  for (const auto& dbSize : dbSizes) {
    TestParams params = {baseDir + "/exp3_db_" + std::to_string(dbSize),
                         dbSize,
                         3,
                         1,
                         100000,
                         1'000'000,
                         6};
    spdlog::info("ExpBloomMetrics: Rozpoczynam eksperyment dla bazy '{}'",
                 params.dbName);

    // dbManager.openDB(params.dbName);

    // if (!initMode) {
    //     dbManager.insertRecords(params.numRecords, columns);
    // }

    StopWatch stopwatch;
    stopwatch.start();
    dbManager.openDB(params.dbName);
    dbManager.insertRecords(params.numRecords, columns);
    stopwatch.stop();
    auto dbCreationTime = stopwatch.elapsedMicros();

    spdlog::info("ExpBloomMetrics: 10 second sleep...");
    std::this_thread::sleep_for(std::chrono::seconds(10));

    stopwatch.start();
    std::map<std::string, BloomTree> hierarchies;
    std::mutex hierarchiesMutex;

    // First asynchronously get all SST files for all columns
    std::map<std::string, std::vector<std::string>> columnSstFiles;
    std::vector<std::future<std::pair<std::string, std::vector<std::string>>>>
        scanFutures;

    for (const auto& column : columns) {
      std::promise<std::pair<std::string, std::vector<std::string>>>
          scanPromise;
      scanFutures.push_back(scanPromise.get_future());

      boost::asio::post(
          globalThreadPool, [column, &dbManager, &params,
                             promise = std::move(scanPromise)]() mutable {
            auto sstFiles =
                dbManager.scanSSTFilesForColumn(params.dbName, column);
            promise.set_value(std::make_pair(column, std::move(sstFiles)));
          });
    }

    // Wait for all scanning to complete
    for (auto& fut : scanFutures) {
      auto [column, sstFiles] = fut.get();
      columnSstFiles[column] = std::move(sstFiles);
    }

    // Now process each column's hierarchy building sequentially
    // (createPartitionedHierarchy already has internal parallelism)
    for (const auto& [column, sstFiles] : columnSstFiles) {
      BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
          sstFiles, params.itemsPerPartition, params.bloomSize,
          params.numHashFunctions, params.bloomTreeRatio);
      spdlog::info("Hierarchy built for column: {}", column);
      hierarchies.try_emplace(column, std::move(hierarchy));
    }

    stopwatch.stop();
    auto bloomCreationTime = stopwatch.elapsedMicros();

    // Zapis wyników do pliku CSV
    std::ofstream out(baseDir + "/exp_3_bloom_metrics.csv", std::ios::app);
    if (!out) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
      return;
    }
    // Format CSV: numRecords, dbSize, bloomCreationTime, dbCreationTime
    out << params.numRecords << "," << dbSize << "," << bloomCreationTime << ","
        << dbCreationTime << "\n";
    out.close();
    dbManager.closeDB();
  }
}