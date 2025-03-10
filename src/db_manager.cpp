#include "db_manager.hpp"
#include "compaction_event_listener.hpp"
#include "stopwatch.hpp"

#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/iterator.h>
#include <rocksdb/table_properties.h>

#include <filesystem>
#include <stdexcept>
#include <spdlog/spdlog.h>

#include <future>

void DBManager::openDB(const std::string& dbname, bool withListener) {
    StopWatch sw;
    sw.start(); // Start timing

    if (db_) {
        spdlog::warn("DB is already open. Closing before reopening.");
        closeDB();
    }

    rocksdb::Options options;
    options.create_if_missing = true;
    // options.disable_auto_compactions = true;
    // options.compression = rocksdb::kNoCompression;
    // options.num_levels = 1;
    //limit number of keys per file
    // options.target_file_size_base = 1000000;
    //prio for compaction
    options.compaction_pri = rocksdb::CompactionPri::kOldestSmallestSeqFirst; 

    if (withListener) {
        options.listeners.emplace_back(std::make_shared<CompactionEventListener>());
    }

    rocksdb::DB* rawDbPtr = nullptr;
    auto status = rocksdb::DB::Open(options, dbname, &rawDbPtr);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open DB: " + status.ToString());
    }

    db_.reset(rawDbPtr);

    sw.stop(); // Stop timing
    spdlog::critical("RocksDB opened at path: {} (withListener={}), took {} µs",
                 dbname, withListener ? "true" : "false", sw.elapsedMicros());
}


void DBManager::insertRecords(int numRecords) {
    StopWatch sw;
    sw.start();

    if (!db_) {
        throw std::runtime_error("Cannot insert: DB is not open. Call openDB() first.");
    }

    for (int i = 0; i < numRecords; ++i) {
        // Minimal debug
        if (i % 500000 == 0) {
            spdlog::debug("Inserted {} records so far...", i);
        }

        const std::string key   = "key" + std::to_string(i);
        // const std::string value = "value" + std::to_string(i);
        //make value 1000 characters long
        const std::string value = "value" + std::to_string(i) + std::string(1000, 'a');

        auto s = db_->Put(rocksdb::WriteOptions(), key, value);
        if (!s.ok()) {
            throw std::runtime_error("RocksDB Put failed: " + s.ToString());
        }
    }

    auto flushStatus = db_->Flush(rocksdb::FlushOptions());
    if (!flushStatus.ok()) {
        throw std::runtime_error("RocksDB flush failed: " + flushStatus.ToString());
    }

    sw.stop();
    spdlog::critical("Successfully inserted {} records (took {} µs).",
                 numRecords, sw.elapsedMicros());

    //manual compaction
    auto compactionStatus = db_->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
    if (!compactionStatus.ok()) {
        throw std::runtime_error("RocksDB compaction failed: " + compactionStatus.ToString());
    }
}

std::vector<std::string> DBManager::scanSSTFiles(const std::string& dbname) {
    StopWatch sw;
    sw.start();

    std::vector<std::string> sstFiles;
    for (const auto& entry : std::filesystem::directory_iterator(dbname)) {
        if (entry.path().extension() == ".sst") {
            sstFiles.push_back(entry.path().string());
        }
    }

    sw.stop();
    spdlog::critical("Found {} SST files in {} (took {} µs).",
                 sstFiles.size(), dbname, sw.elapsedMicros());
    return sstFiles;
}

bool DBManager::checkValueInHierarchy(bloomTree& hierarchy, const std::string& value) {
    StopWatch sw;
    sw.start();

    auto candidates = hierarchy.checkExistance(value);
    if (candidates.empty()) {
        spdlog::debug("Bloom filter indicates \"{}\" not present in any SST.", value);
        sw.stop();
        spdlog::critical("checkValueInHierarchy took {} µs.", sw.elapsedMicros());
        return false;
    }

    spdlog::debug("Value \"{}\" might exist in the following SSTables:", value);
    for (const auto& candidate : candidates) {
        spdlog::debug("  - {}", candidate);
    }

    // Actually scan each candidate file to confirm
    std::vector<std::future<bool>> futures;
    std::atomic<bool> found{false};

    for (const auto& file : candidates) {
        futures.emplace_back(std::async(std::launch::async, [this, &file, &value, &found]() {
            if (found.load()) {
                return false;
            }
            bool result = ScanFileForValue(file, value);
            if (result) {
                found.store(true);
            }
            return result;
        }));
    }

    while (!futures.empty()) {
        for (auto it = futures.begin(); it != futures.end(); ) {
            if (it->wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
                if (it->get()) {
                    spdlog::debug("Value \"{}\" truly found in one of the files.", value);
                    sw.stop();
                    spdlog::critical("checkValueInHierarchy took {} µs.", sw.elapsedMicros());
                    return true;
                }
                it = futures.erase(it);
            } else {
                ++it;
            }
        }
    }

    spdlog::debug("None of the candidate files contained the actual value \"{}\".", value);
    sw.stop();
    spdlog::critical("checkValueInHierarchy took {} µs.", sw.elapsedMicros());
    return false;
}
bool DBManager::checkValueInHierarchyWithoutConcurrency(bloomTree& hierarchy, const std::string& value) {
    StopWatch sw;
    sw.start();

    auto candidates = hierarchy.checkExistance(value);
    if (candidates.empty()) {
        spdlog::debug("Bloom filter indicates \"{}\" not present in any SST.", value);
        sw.stop();
        spdlog::critical("checkValueInHierarchyWithoutConcurrency took {} µs.", sw.elapsedMicros());
        return false;
    }

    spdlog::debug("Value \"{}\" might exist in the following SSTables:", value);
    for (const auto& candidate : candidates) {
        spdlog::debug("  - {}", candidate);
    }

    // Actually scan each candidate file to confirm
    for (const auto& file : candidates) {
        spdlog::debug("Scanning file: {} for value: {}", file, value);
        if (ScanFileForValue(file, value)) {
            spdlog::debug("Value \"{}\" truly found in one of the files.", value);
            sw.stop();
            spdlog::critical("checkValueInHierarchyWithoutConcurrency took {} µs.", sw.elapsedMicros());
            return true;
        }
    }

    spdlog::debug("None of the candidate files contained the actual value \"{}\".", value);
    sw.stop();
    spdlog::critical("checkValueInHierarchyWithoutConcurrency took {} µs.", sw.elapsedMicros());
    return false;
}

bool DBManager::checkValueWithoutBloomFilters(const std::string& value) {
    StopWatch sw;
    sw.start();

    if (!db_) {
        throw std::runtime_error("Cannot check value: DB is not open.");
    }

    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = false;
    readOptions.verify_checksums = true;

    sw.stop();
    spdlog::critical("checkValueWithoutBloomFilters setup took {} µs.", sw.elapsedMicros());

    sw.start();
    // Create an iterator to scan all K-V pairs in the open DB
    std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(readOptions));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (iter->value().ToString() == value) {
            sw.stop();
            spdlog::critical("checkValueWithoutBloomFilters took {} µs (found).", sw.elapsedMicros());
            return true;
        }
    }
    sw.stop();
    spdlog::critical("checkValueWithoutBloomFilters took {} µs (not found).", sw.elapsedMicros());
    return false;
}

void DBManager::closeDB() {
    StopWatch sw;
    sw.start();

    if (db_) {
        db_.reset();
        spdlog::debug("DB closed.");
    }

    sw.stop();
    spdlog::critical("closeDB took {} µs.", sw.elapsedMicros());
}

bool DBManager::ScanFileForValue(const std::string& filename, const std::string& value) {
    StopWatch sw;

    rocksdb::Options options;
    options.env = rocksdb::Env::Default();

    rocksdb::SstFileReader reader(options);
    rocksdb::Status status = reader.Open(filename);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open SSTable: " + status.ToString());
    }

    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = false;
    readOptions.verify_checksums = true;

    auto iter = std::unique_ptr<rocksdb::Iterator>(reader.NewIterator(readOptions));

    sw.start();

    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (iter->value().ToString() == value) {
            sw.stop();
            spdlog::critical("ScanFileForValue({}) found value. Took {} µs.",
                          filename, sw.elapsedMicros());
            return true;
        }
    }

    sw.stop();
    spdlog::critical("ScanFileForValue({}) did not find value. Took {} µs.",
                  filename, sw.elapsedMicros());
    return false;
}
