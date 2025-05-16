#include <spdlog/spdlog.h>

#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <map>
#include <regex>
#include <string>
#include <thread>
#include <vector>
#include <random>

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

boost::asio::thread_pool globalThreadPool{std::thread::hardware_concurrency()};

// Test parameter structure.
struct TestParams {
    std::string dbName;
    bool compactionLogging;
    int numRecords;
    int bloomTreeRatio;
    int numberOfAttempts;
    size_t itemsPerPartition;
    size_t bloomSize;
    int numHashFunctions;
};

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

        if (entry.is_regular_file() && std::regex_match(filename, bloomFilePattern)) {
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
    bool initMode = false;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--init") {
            initMode = true;
            break;
        }
    }
    try {
        // run section
        // runExp1(baseDir, initMode);
        // runExp2(baseDir, initMode);
        // runExp3(baseDir, initMode);
        // runExp4(baseDir, initMode);
        // runExp5(baseDir, initMode);
        // runExp6(baseDir, initMode);
        runExp7(baseDir, initMode);
        runExp8(baseDir, initMode);
    } catch (const std::exception& e) {
        spdlog::error("[Error] {}", e.what());
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
