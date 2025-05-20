#include "exp2.hpp"

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

extern void clearBloomFilterFiles(const std::string& dbDir);
extern boost::asio::thread_pool globalThreadPool;

TestParams buildParams(const std::string& dbName, size_t items, size_t dbSize) {
  return TestParams{
      dbName,
      static_cast<int>(dbSize),
      3,          // bloomTreeRatio
      1,          // numberOfAttempts
      items,      // itemsPerPartition
      1'000'000,  // bloomSize
      6           // numHashFunctions
  };
}

void runExp2(const std::string& dbPath, size_t dbSize) {
  const std::vector<std::string> columns = {"phone", "mail", "address"};
  const std::vector<size_t> itemsPerPartition = {50000, 100000, 200000, 500000,
                                                 1000000};

  writeCSVheaders();

  DBManager dbManager;
  BloomManager bloomManager;

  spdlog::info("Exp2: Opening database '{}' for experiment.", dbPath);
  dbManager.openDB(dbPath);

  for (const auto& items : itemsPerPartition) {
    TestParams params = buildParams(dbPath, items, dbSize);
    spdlog::info(
        "Exp2: Running iteration with items_per_partition={} on database '{}'",
        items, params.dbName);
    clearBloomFilterFiles(params.dbName);

    std::map<std::string, BloomTree> hierarchies;

    std::map<std::string, std::vector<std::string>> columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);

    hierarchies = buildHierarchies(columnSstFiles, bloomManager, params);

    size_t totalDiskBloomSize = 0;
    size_t totalMemoryBloomSize = 0;
    for (const auto& kv : hierarchies) {
      const BloomTree& tree = kv.second;
      totalDiskBloomSize += tree.diskSize();
      totalMemoryBloomSize += tree.memorySize();
    }

    // Zapis wyników do pliku CSV
    std::ofstream out("csv/exp_2_bloom_metrics.csv", std::ios::app);
    if (!out) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
      return;
    }
    out << dbSize << "," << items << ","
        << hierarchies.at(columns[0]).leafNodes.size() << ","
        << getProbabilityOfFalsePositive(params.bloomSize,
                                         params.numHashFunctions,
                                         params.itemsPerPartition)
        << "," << totalDiskBloomSize << "," << totalMemoryBloomSize << "\n";
    out.close();
  }
  spdlog::info("ExpBloomMetrics: Closing database '{}'.", dbPath);
  dbManager.closeDB();  // Close DB once after all iterations
}

void writeCSVheaders() {
  writeCsvHeader("csv/exp_2_bloom_metrics.csv", 
                 "dbSize,itemsPerPartition,leafs,falsePositive,diskBloomSize,memoryBloomSize");
}