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

void initializeSharedDatabase(const std::string& dbName,
                              const std::vector<std::string>& columns,
                              int numRecords, bool performInit) {
    if (performInit) {
        spdlog::info("MAIN: Initializing database '{}' with {} records.", dbName, numRecords);
        DBManager dbManager;
        dbManager.openDB(dbName);
        clearBloomFilterFiles(dbName);
        dbManager.insertRecords(numRecords, columns);
        spdlog::info("MAIN: Completed record insertion for '{}'. Sleeping for 10s.", dbName);
        std::this_thread::sleep_for(std::chrono::seconds(10));
        dbManager.compactAllColumnFamilies();
        dbManager.closeDB();
        spdlog::info("MAIN: Database '{}' initialization complete and closed.", dbName);
    } else {
        spdlog::info("MAIN: Skipping initialization for database '{}'.", dbName);
    }
}

// ##### Main function ####
int main(int argc, char* argv[]) {
  const std::string baseDir = "db";
  if (!std::filesystem::exists(baseDir)) {
    std::filesystem::create_directory(baseDir);
  }
  bool initMode = false;
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--db") {
      initMode = true;
      break;
    }
  }

  const std::string sharedDbName = baseDir + "/shared_exp_db";
  const std::vector<std::string> defaultColumns = {"phone", "mail", "address"}; // Example default columns
  const int defaultNumRecords = 20000000; // Example default record count, from TestParams

  initializeSharedDatabase(sharedDbName, defaultColumns, defaultNumRecords, initMode);
  
  try {
    // run section
    // runExp1(baseDir, initMode);
    // runExp2(sharedDbName, defaultNumRecords); // Pass sharedDbName, no DBManager
    // runExp3(baseDir, initMode, sharedDbName); // TODO: Adapt runExp3 similarly
    // runExp4(baseDir, initMode, sharedDbName); // TODO: Adapt runExp4 similarly
    runExp5(sharedDbName, defaultNumRecords);
    runExp6(sharedDbName, defaultNumRecords);
    // runExp7(baseDir, initMode);
    // runExp8(baseDir, initMode);
  } catch (const std::exception& e) {
    spdlog::error("[Error] {}", e.what());
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
