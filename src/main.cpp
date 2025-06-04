#include <spdlog/spdlog.h>

#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <map>
#include <random>
#include <regex>
#include <string>
#include <thread>
#include <vector>

#include "bloom_manager.hpp"
#include "db_manager.hpp"
#include "exp1.hpp"
#include "exp2.hpp"
#include "exp3.hpp"
#include "exp4.hpp"
#include "exp5.hpp"
#include "exp6.hpp"
#include "exp7.hpp"
#include "exp8.hpp"
#include "stopwatch.hpp"
#include "test_params.hpp"

boost::asio::thread_pool globalThreadPool{std::thread::hardware_concurrency()};

void clearBloomFilterFiles(const std::string& dbDir) {
  std::regex bloomFilePattern(R"(^\d+\.sst_[^_]+_[^_]+$)");
  std::error_code ec;

  for (auto const& entry : std::filesystem::directory_iterator(dbDir, ec)) {
    if (ec) {
      spdlog::error("Failed to iterate '{}': {}", dbDir, ec.message());
      return;
    }

    auto const& path = entry.path();
    auto filename = path.filename().string();

    if (entry.is_regular_file() &&
        std::regex_match(filename, bloomFilePattern)) {
      if (std::filesystem::remove(path, ec)) {
      } else {
        spdlog::error("Could not remove '{}': {}", path.string(), ec.message());
      }
    }
  }
}

// ##### Main function ####
int main(int argc, char* argv[]) {
  const std::string baseDir = "db";
  if (!std::filesystem::exists(baseDir)) {
    std::filesystem::create_directory(baseDir);
  }
  const std::string csvDir = "csv";
  if (!std::filesystem::exists(csvDir)) {
    std::filesystem::create_directory(csvDir);
  }
  bool initMode = false;
  bool skipDbScan = false;
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--build-db") {
      initMode = true;
    } else if (std::string(argv[i]) == "--skip-scan") {
      skipDbScan = true;
    }
  }

  const std::string sharedDbName = baseDir + "/shared_exp_db";
  const std::vector<std::string> defaultColumns = {"phone", "mail", "address"};
  const int defaultNumRecords = 20000000;

  try {
    // run section
    runExp1(baseDir, initMode, sharedDbName, defaultNumRecords, skipDbScan);
    // EXP 2 included in exp5 run
    // EXP 3 included in exp1 first run (creating DB)
    // EXP 4 included in exp1 first run (running queries)
    runExp5(sharedDbName, defaultNumRecords, skipDbScan);
    // runExp6(sharedDbName, defaultNumRecords, skipDbScan);
    // runExp7(sharedDbName, defaultNumRecords, skipDbScan);
    // runExp8(baseDir, initMode, skipDbScan);
  } catch (const std::exception& e) {
    spdlog::error("[Error] {}", e.what());
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
