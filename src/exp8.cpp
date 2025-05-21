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
  const std::vector<int> numColumnsToTest = {2,3,4,5,6,7,8};
  const int maxColumns = 10;
  const std::string fixedDbName = baseDir + "/exp8_shared_db";

  std::vector<std::string> allColumnNames;
  for (int i = 0; i < maxColumns; ++i) {
    allColumnNames.push_back("i_" + std::to_string(i) + "_column");
  }

  DBManager dbManager;
  BloomManager bloomManager;

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

    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, currentColumns, dbSize, 10, skipDbScan);

    std::ofstream out_csv("csv/exp_8_bloom_metrics.csv", std::ios::app);
    if (!out_csv) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
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

    dbManager.closeDB();
  }
}