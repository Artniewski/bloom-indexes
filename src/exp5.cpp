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

void writeExp5BasicTimingsHeaders() {
  writeCsvHeader("csv/exp_5_basic_timings.csv",
                 "numRecords,itemsPerPartition,falsePositiveProbability,"
                 "globalScanTime,hierarchicalSingleTime,hierarchicalMultiTime");
}

void writeExp5BasicChecksHeaders() {
  writeCsvHeader("csv/exp_5_basic_checks.csv",
                 "numRecords,itemsPerPartition,"
                 "multiBloomChecks,multiLeafBloomChecks,multiSSTChecks,"
                 "singleBloomChecks,singleLeafBloomChecks,singleSSTChecks");
}

void writeExp5PerColumnMetricsHeaders() {
  writeCsvHeader("csv/exp_5_per_column_metrics.csv",
                 "numRecords,itemsPerPartition,numColumns,"
                 "multiBloomPerCol,multiLeafPerCol,multiNonLeafPerCol,multiSSTPerCol");
}

void writeExp5RealDataChecksHeaders() {
  writeCsvHeader("csv/exp_5_real_data_checks.csv",
                 "numRecords,itemsPerPartition,realDataPercentage,"
                 "avgMultiBloomChecks,avgMultiLeafBloomChecks,avgMultiNonLeafBloomChecks,avgMultiSSTChecks,"
                 "avgSingleBloomChecks,avgSingleLeafBloomChecks,avgSingleNonLeafBloomChecks,avgSingleSSTChecks,"
                 "avgRealMultiBloomChecks,avgRealMultiSSTChecks,avgFalseMultiBloomChecks,avgFalseMultiSSTChecks");
}

void writeExp5RealDataPerColumnHeaders() {
  writeCsvHeader("csv/exp_5_real_data_per_column.csv",
                 "numRecords,itemsPerPartition,realDataPercentage,numColumns,"
                 "avgMultiBloomPerCol,avgMultiLeafPerCol,avgMultiNonLeafPerCol,avgMultiSSTPerCol,"
                 "avgRealMultiBloomPerCol,avgRealMultiSSTPerCol,avgFalseMultiBloomPerCol,avgFalseMultiSSTPerCol");
}

void writeExp5PartitionEfficiencyHeaders() {
  writeCsvHeader("csv/exp_5_partition_efficiency.csv",
                 "numRecords,itemsPerPartition,realDataPercentage,falsePositiveProbability,"
                 "avgMultiTime,avgSingleTime,avgMultiBloomPerCol,avgMultiSSTPerCol");
}

void writeExp5TimingComparisonHeaders() {
  writeCsvHeader("csv/exp_5_timing_comparison.csv",
                 "numRecords,itemsPerPartition,realDataPercentage,"
                 "avgRealMultiTime,avgRealSingleTime,avgFalseMultiTime,avgFalseSingleTime");
}

void runExp5(const std::string& dbPath, size_t dbSizeParam, bool skipDbScan) {
  const std::vector<std::string> columns = {"phone", "mail", "address"};
  const size_t bloomFilterSize = 4'000'000;
  const std::vector<size_t> itemsPerPartitionVec = {100000, 150000, 200000};
  const int numQueryRuns = 100;

  // Initialize CSV headers
  writeExp5BasicTimingsHeaders();
  writeExp5BasicChecksHeaders();
  writeExp5PerColumnMetricsHeaders();
  writeExp5RealDataChecksHeaders();
  writeExp5RealDataPerColumnHeaders();
  writeExp5PartitionEfficiencyHeaders();
  writeExp5TimingComparisonHeaders();

  DBManager dbManager;
  BloomManager bloomManager;

  for (const auto& currentItemsPerPartition : itemsPerPartitionVec) {
    TestParams params = {dbPath, static_cast<int>(dbSizeParam), 3,
                         1, currentItemsPerPartition, bloomFilterSize, 3};
    spdlog::info("Exp5: Running for DB: '{}', itemsPerPartition: {}",
                 params.dbName, currentItemsPerPartition);

    clearBloomFilterFiles(params.dbName);
    dbManager.openDB(params.dbName);

    std::map<std::string, std::vector<std::string>> columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);

    std::map<std::string, BloomTree> hierarchies =
        buildHierarchies(columnSstFiles, bloomManager, params);

    // Run standard queries first
    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, columns, dbSizeParam, numQueryRuns, skipDbScan);

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

    // Write basic performance metrics
    std::ofstream basic_timings("csv/exp_5_basic_timings.csv", std::ios::app);
    if (basic_timings) {
      basic_timings << params.numRecords << "," << currentItemsPerPartition << "," << falsePositiveProb << ","
                    << timings.globalScanTimeStats.average << ","
                    << timings.hierarchicalSingleTimeStats.average << ","
                    << timings.hierarchicalMultiTimeStats.average << "\n";
      basic_timings.close();
    }

    std::ofstream basic_checks("csv/exp_5_basic_checks.csv", std::ios::app);
    if (basic_checks) {
      basic_checks << params.numRecords << "," << currentItemsPerPartition << ","
                   << timings.multiCol_bloomChecksStats.average << ","
                   << timings.multiCol_leafBloomChecksStats.average << ","
                   << timings.multiCol_sstChecksStats.average << ","
                   << timings.singleCol_bloomChecksStats.average << ","
                   << timings.singleCol_leafBloomChecksStats.average << ","
                   << timings.singleCol_sstChecksStats.average << "\n";
      basic_checks.close();
    }

    std::ofstream per_column_metrics("csv/exp_5_per_column_metrics.csv", std::ios::app);
    if (per_column_metrics) {
      per_column_metrics << params.numRecords << "," << currentItemsPerPartition << "," << columns.size() << ","
                         << timings.multiCol_bloomChecksPerColumnStats.average << ","
                         << timings.multiCol_leafBloomChecksPerColumnStats.average << ","
                         << timings.multiCol_nonLeafBloomChecksPerColumnStats.average << ","
                         << timings.multiCol_sstChecksPerColumnStats.average << "\n";
      per_column_metrics.close();
    }

    // Run comprehensive analysis for real data percentage studies
    const int numQueriesPerScenario = 100;
    
    spdlog::info("Exp5: Running comprehensive analysis for {} columns with {} queries per scenario", 
                 columns.size(), numQueriesPerScenario);
    std::vector<AccumulatedQueryMetrics> comprehensiveResults = runComprehensiveQueryAnalysis(
        dbManager, hierarchies, columns, dbSizeParam, numQueriesPerScenario);
    
    spdlog::info("Exp5: Generated {} comprehensive analysis results for {} columns", 
                 comprehensiveResults.size(), columns.size());

    // Write comprehensive analysis results to focused CSV files
    std::ofstream real_data_checks("csv/exp_5_real_data_checks.csv", std::ios::app);
    std::ofstream real_data_per_column("csv/exp_5_real_data_per_column.csv", std::ios::app);
    std::ofstream partition_efficiency("csv/exp_5_partition_efficiency.csv", std::ios::app);
    std::ofstream timing_comparison("csv/exp_5_timing_comparison.csv", std::ios::app);

    for (const auto& result : comprehensiveResults) {
      // Real data checks (15 columns)
      if (real_data_checks) {
        real_data_checks << params.numRecords << "," << currentItemsPerPartition << "," << result.realDataPercentage << ","
                         << result.avgMultiBloomChecks << "," << result.avgMultiLeafBloomChecks << ","
                         << result.avgMultiNonLeafBloomChecks << "," << result.avgMultiSSTChecks << ","
                         << result.avgSingleBloomChecks << "," << result.avgSingleLeafBloomChecks << ","
                         << result.avgSingleNonLeafBloomChecks << "," << result.avgSingleSSTChecks << ","
                         << result.avgRealMultiBloomChecks << "," << result.avgRealMultiSSTChecks << ","
                         << result.avgFalseMultiBloomChecks << "," << result.avgFalseMultiSSTChecks << "\n";
      }

      // Real data per-column (12 columns) - only multi-column gets per-column treatment
      if (real_data_per_column) {
        real_data_per_column << params.numRecords << "," << currentItemsPerPartition << "," << result.realDataPercentage << "," << result.numColumns << ","
                             << result.avgMultiBloomChecksPerColumn << "," << result.avgMultiLeafBloomChecksPerColumn << ","
                             << result.avgMultiNonLeafBloomChecksPerColumn << "," << result.avgMultiSSTChecksPerColumn << ","
                             << result.avgRealMultiBloomChecksPerColumn << "," << result.avgRealMultiSSTChecksPerColumn << ","
                             << result.avgFalseMultiBloomChecksPerColumn << "," << result.avgFalseMultiSSTChecksPerColumn << "\n";
      }

      // Partition efficiency summary (8 columns)
      if (partition_efficiency) {
        partition_efficiency << params.numRecords << "," << currentItemsPerPartition << "," << result.realDataPercentage << "," << falsePositiveProb << ","
                             << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << ","
                             << result.avgMultiBloomChecksPerColumn << "," << result.avgMultiSSTChecksPerColumn << "\n";
      }

      // Timing comparison (7 columns)
      if (timing_comparison) {
        timing_comparison << params.numRecords << "," << currentItemsPerPartition << "," << result.realDataPercentage << ","
                         << result.avgRealDataMultiTime << "," << result.avgRealDataSingleTime << ","
                         << result.avgFalseDataMultiTime << "," << result.avgFalseDataSingleTime << "\n";
      }
    }

    real_data_checks.close();
    real_data_per_column.close();
    partition_efficiency.close();
    timing_comparison.close();

    // Write bloom metrics for reference
    std::ofstream bloom_metrics("csv/exp_5_bloom_metrics.csv", std::ios::app);
    if (bloom_metrics && currentItemsPerPartition == itemsPerPartitionVec[0]) {
      bloom_metrics << "dbSize,itemsPerPartition,falsePositiveProbability,leafs,diskBloomSize,memoryBloomSize\n";
    }
    if (bloom_metrics) {
      bloom_metrics << params.numRecords << "," << currentItemsPerPartition << ","
                    << falsePositiveProb << "," << leafs << "," << totalDiskBloomSize << ","
                    << totalMemoryBloomSize << "\n";
      bloom_metrics.close();
    }

    dbManager.closeDB();
  }
}