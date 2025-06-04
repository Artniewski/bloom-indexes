#include "exp7.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <tuple>
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
extern std::atomic<size_t> gBloomCheckCount;
extern std::atomic<size_t> gLeafBloomCheckCount;
extern std::atomic<size_t> gSSTCheckCount;

void writeExp7ChecksCSVHeaders() {
  writeCsvHeader(
      "csv/exp_7_checks.csv",
      "numRecords,keys,sstFiles,"
      "multiCol_bloomChecks_avg,multiCol_bloomChecks_min,multiCol_bloomChecks_max,"
      "multiCol_leafBloomChecks_avg,multiCol_leafBloomChecks_min,multiCol_leafBloomChecks_max,"
      "multiCol_sstChecks_avg,multiCol_sstChecks_min,multiCol_sstChecks_max,"
      "singleCol_bloomChecks_avg,singleCol_bloomChecks_min,singleCol_bloomChecks_max,"
      "singleCol_leafBloomChecks_avg,singleCol_leafBloomChecks_min,singleCol_leafBloomChecks_max,"
      "singleCol_sstChecks_avg,singleCol_sstChecks_min,singleCol_sstChecks_max");
}

void writeExp7DerivedMetricsCSVHeaders() {
  writeCsvHeader(
      "csv/exp_7_derived_metrics.csv",
      "numRecords,keys,sstFiles,"
      "multiCol_nonLeafBloomChecks_avg,multiCol_nonLeafBloomChecks_min,multiCol_nonLeafBloomChecks_max,"
      "singleCol_nonLeafBloomChecks_avg,singleCol_nonLeafBloomChecks_min,singleCol_nonLeafBloomChecks_max");
}

void writeExp7PerColumnCSVHeaders() {
  writeCsvHeader(
      "csv/exp_7_per_column.csv",
      "numRecords,keys,sstFiles,numColumns,"
      "multiCol_bloomChecksPerColumn_avg,multiCol_bloomChecksPerColumn_min,multiCol_bloomChecksPerColumn_max,"
      "multiCol_leafBloomChecksPerColumn_avg,multiCol_leafBloomChecksPerColumn_min,multiCol_leafBloomChecksPerColumn_max,"
      "multiCol_nonLeafBloomChecksPerColumn_avg,multiCol_nonLeafBloomChecksPerColumn_min,multiCol_nonLeafBloomChecksPerColumn_max,"
      "multiCol_sstChecksPerColumn_avg,multiCol_sstChecksPerColumn_min,multiCol_sstChecksPerColumn_max");
}

void writeExp7TimingsCSVHeaders() {
  writeCsvHeader("csv/exp_7_timings.csv",
                 "numRecords,keys,"
                 "hierarchicalSingleTime_avg,hierarchicalSingleTime_min,"
                 "hierarchicalSingleTime_max,"
                 "hierarchicalMultiTime_avg,hierarchicalMultiTime_min,"
                 "hierarchicalMultiTime_max");
}

void writeExp7OverviewCSVHeaders() {
  writeCsvHeader("csv/exp_7_overview.csv",
                 "numRecords,keys,falsePositiveProbability,"
                 "globalScanTime_avg,hierarchicalSingleTime_avg,"
                 "hierarchicalMultiTime_avg");
}

void writeExp7SelectedAvgChecksCSVHeaders() {
  writeCsvHeader("csv/exp_7_selected_avg_checks.csv",
                 "numRec,keys,"
                 "mcBloomAvg,mcLeafAvg,mcNonLeafAvg,mcSSTAvg,"
                 "scBloomAvg,scLeafAvg,scNonLeafAvg,scSSTAvg");
}

void runExp7(const std::string& dbPathToUse, size_t dbSizeToUse,
             bool skipDbScan) {
  const std::vector<std::string> columns = {"phone", "mail", "address"};
  const std::vector<int> targetItemsLoopVar = {2, 4, 6, 8, 10};
  std::vector<int> targetRecordIndices;
  generateRandomIndexes(dbSizeToUse, 10, targetRecordIndices);

  TestParams params = {
      dbPathToUse,
      static_cast<int>(dbSizeToUse),
      3,        // bloomTreeRatio - default or from a config
      1,        // numberOfAttempts - default or from a config
      100000,   // itemsPerPartition - default or from a config
      4000000,  // bloomSize - default or from a config
      3         // numHashFunctions - default or from a config
  };
  DBManager dbManager;
  BloomManager bloomManager;

  writeExp7ChecksCSVHeaders();
  writeExp7DerivedMetricsCSVHeaders();
  writeExp7PerColumnCSVHeaders();
  writeExp7TimingsCSVHeaders();
  writeExp7OverviewCSVHeaders();
  writeExp7SelectedAvgChecksCSVHeaders();

  for (const auto& numTargetRecords : targetItemsLoopVar) {
    dbManager.openDB(params.dbName, columns);
    std::vector<std::tuple<std::string, std::string, std::string>>
        originalDataToRevert;
    std::vector<std::tuple<std::string, std::string, std::string>>
        modificationsToApply;
    std::vector<std::string> currentExpectedValues;

    for (int i = 0; i < numTargetRecords; i++) {
      int recordIndex = targetRecordIndices[i];
      std::string currentKey =
          createPrefixedKeyExp7(recordIndex, params.numRecords);
      for (const auto& column : columns) {
        try {
          std::string originalValue = dbManager.getValue(column, currentKey);
          originalDataToRevert.emplace_back(currentKey, column, originalValue);
          spdlog::info("Exp7: Stored original for key '{}', col '{}': '{}'",
                       currentKey, column, originalValue);
        } catch (const std::exception& e) {
          spdlog::warn(
              "Exp7: Failed to get original value for key '{}', col '{}': {}. "
              "Storing empty for revert.",
              currentKey, column, e.what());
          originalDataToRevert.emplace_back(currentKey, column, "");
        }
        std::string targetValue = column + "_target";
        modificationsToApply.emplace_back(currentKey, column, targetValue);
        currentExpectedValues.push_back(targetValue);
      }
    }

    spdlog::info("Exp7: Applying modifications to DB...");
    rocksdb::Status s_modify =
        dbManager.applyModifications(modificationsToApply, params.numRecords);
    if (!s_modify.ok()) {
      spdlog::error("Exp7: Failed to apply modifications to target records: {}",
                    s_modify.ToString());
      dbManager.closeDB();
      return;
    }

    clearBloomFilterFiles(params.dbName);

    std::map<std::string, BloomTree> hierarchies;

    std::map<std::string, std::vector<std::string>> columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);

    hierarchies = buildHierarchies(columnSstFiles, bloomManager, params);
    std::vector<std::string> targetColumns;
    for (const auto& column : columns) {
      targetColumns.push_back(column + "_target");
    }
    AggregatedQueryTimings timings =
        runStandardQueriesWithTarget(dbManager, hierarchies, columns,
                                     dbSizeToUse, 1, skipDbScan, targetColumns);

    double falsePositiveProb = getProbabilityOfFalsePositive(
        params.bloomSize, params.numHashFunctions, params.itemsPerPartition);

    std::ofstream checks_csv_out("csv/exp_7_checks.csv", std::ios::app);
    if (!checks_csv_out) {
      spdlog::error(
          "Exp7: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_7_checks.csv do dopisywania!");
      return;
    }
    std::ofstream derived_csv_out("csv/exp_7_derived_metrics.csv", std::ios::app);
    if (!derived_csv_out) {
      spdlog::error(
          "Exp7: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_7_derived_metrics.csv do dopisywania!");
      return;
    }
    std::ofstream per_column_csv_out("csv/exp_7_per_column.csv", std::ios::app);
    if (!per_column_csv_out) {
      spdlog::error(
          "Exp7: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_7_per_column.csv do dopisywania!");
      return;
    }
    std::ofstream timings_csv_out("csv/exp_7_timings.csv", std::ios::app);
    if (!timings_csv_out) {
      spdlog::error(
          "Exp7: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_7_timings.csv do dopisywania!");
      return;
    }
    std::ofstream overview_csv_out("csv/exp_7_overview.csv", std::ios::app);
    if (!overview_csv_out) {
      spdlog::error(
          "Exp7: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_7_overview.csv do dopisywania!");
      return;
    }
    std::ofstream selected_avg_checks_csv_out(
        "csv/exp_7_selected_avg_checks.csv", std::ios::app);
    if (!selected_avg_checks_csv_out) {
      spdlog::error(
          "Exp7: Nie udało się otworzyć pliku wynikowego "
          "csv/exp_7_selected_avg_checks.csv do dopisywania!");
      return;
    }
    size_t countSSTFiles = 0;
    for (const auto& column : columns) {
      countSSTFiles += columnSstFiles[column].size();
    }
    checks_csv_out << params.numRecords << "," << numTargetRecords << ","
                   << countSSTFiles << ","
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
                   << timings.singleCol_nonLeafBloomChecksStats.average << ","
                   << timings.singleCol_nonLeafBloomChecksStats.min << ","
                   << timings.singleCol_nonLeafBloomChecksStats.max << ","
                   << timings.singleCol_sstChecksStats.average << ","
                   << timings.singleCol_sstChecksStats.min << ","
                   << timings.singleCol_sstChecksStats.max << "\n";

    derived_csv_out << params.numRecords << "," << numTargetRecords << ","
                    << countSSTFiles << ","
                    << timings.multiCol_nonLeafBloomChecksStats.average << ","
                    << timings.multiCol_nonLeafBloomChecksStats.min << ","
                    << timings.multiCol_nonLeafBloomChecksStats.max << ","
                    << timings.singleCol_nonLeafBloomChecksStats.average << ","
                    << timings.singleCol_nonLeafBloomChecksStats.min << ","
                    << timings.singleCol_nonLeafBloomChecksStats.max << "\n";

    per_column_csv_out << params.numRecords << "," << numTargetRecords << ","
                       << countSSTFiles << "," << columns.size() << ","
                       << timings.multiCol_bloomChecksPerColumnStats.average << ","
                       << timings.multiCol_bloomChecksPerColumnStats.min << ","
                       << timings.multiCol_bloomChecksPerColumnStats.max << ","
                       << timings.multiCol_leafBloomChecksPerColumnStats.average << ","
                       << timings.multiCol_leafBloomChecksPerColumnStats.min << ","
                       << timings.multiCol_leafBloomChecksPerColumnStats.max << ","
                       << timings.multiCol_nonLeafBloomChecksPerColumnStats.average << ","
                       << timings.multiCol_nonLeafBloomChecksPerColumnStats.min << ","
                       << timings.multiCol_nonLeafBloomChecksPerColumnStats.max << ","
                       << timings.multiCol_sstChecksPerColumnStats.average << ","
                       << timings.multiCol_sstChecksPerColumnStats.min << ","
                       << timings.multiCol_sstChecksPerColumnStats.max << "\n";

    timings_csv_out << params.numRecords << "," << numTargetRecords << ","
                    << timings.hierarchicalSingleTimeStats.average << ","
                    << timings.hierarchicalSingleTimeStats.min << ","
                    << timings.hierarchicalSingleTimeStats.max << ","
                    << timings.hierarchicalMultiTimeStats.average << ","
                    << timings.hierarchicalMultiTimeStats.min << ","
                    << timings.hierarchicalMultiTimeStats.max << "\n";

    overview_csv_out << params.numRecords << "," << numTargetRecords << ","
                     << falsePositiveProb << ","
                     << timings.globalScanTimeStats.average << ","
                     << timings.hierarchicalSingleTimeStats.average << ","
                     << timings.hierarchicalMultiTimeStats.average << "\n";

    selected_avg_checks_csv_out
        << params.numRecords << "," << numTargetRecords << ","
        << timings.multiCol_bloomChecksStats.average << ","
        << timings.multiCol_leafBloomChecksStats.average << ","
        << timings.multiCol_nonLeafBloomChecksStats.average << ","
        << timings.multiCol_sstChecksStats.average << ","
        << timings.singleCol_bloomChecksStats.average << ","
        << timings.singleCol_leafBloomChecksStats.average << ","
        << timings.singleCol_nonLeafBloomChecksStats.average << ","
        << timings.singleCol_sstChecksStats.average << "\n";

    dbManager.revertModifications(originalDataToRevert, params.numRecords);
    dbManager.closeDB();
    checks_csv_out.close();
    derived_csv_out.close();
    per_column_csv_out.close();
    timings_csv_out.close();
    overview_csv_out.close();
    selected_avg_checks_csv_out.close();
  }
}

void generateRandomIndexes(size_t dbSize, const int numTargetRecords,
                           std::vector<int>& targetRecordIndices) {
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(1, static_cast<int>(dbSize));
    std::unordered_set<int> unique_indices_set;
    while (unique_indices_set.size() < numTargetRecords) {
      unique_indices_set.insert(dist(gen));
    }
    targetRecordIndices.assign(unique_indices_set.begin(),
                               unique_indices_set.end());
    std::string indicesStr;
    for (int idx : targetRecordIndices) {
      indicesStr += std::to_string(idx) + ", ";
    }
    spdlog::info("Exp7: Generated {} target record indices: {}",
                 targetRecordIndices.size(), indicesStr);
  }
}

std::string createPrefixedKeyExp7(int recordIndex, int totalRecords) {
  std::string indexStr = std::to_string(recordIndex);
  int paddingLength = 20;
  std::string prefix = "key";
  if (indexStr.length() < paddingLength) {
    return prefix + std::string(paddingLength - indexStr.length(), '0') +
           indexStr;
  }
  return prefix + indexStr;
}
