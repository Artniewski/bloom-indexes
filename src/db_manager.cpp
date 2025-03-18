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
    sw.start();

    if (db_) {
        spdlog::warn("DB already open, closing before reopening.");
        closeDB();
    }

    rocksdb::DBOptions dbOptions;
    dbOptions.create_if_missing = true;
    dbOptions.create_missing_column_families = true;
    if (withListener) {
        dbOptions.listeners.emplace_back(std::make_shared<CompactionEventListener>());
    }

    // Define 5 hardcoded column families
    std::vector<std::string> cf_names = {"default", "phone", "mail", "address", "name", "surname"};
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
    for (const auto& name : cf_names) {
        cf_descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
    }

    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_raw;
    rocksdb::DB* rawDbPtr = nullptr;

    auto status = rocksdb::DB::Open(dbOptions, dbname, cf_descriptors, &cf_handles_raw, &rawDbPtr);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open DB with Column Families: " + status.ToString());
    }

    db_.reset(rawDbPtr);

    // Store ColumnFamilyHandles in a map for easy access
    cf_handles_.clear();
    for (size_t i = 0; i < cf_names.size(); ++i) {
        cf_handles_[cf_names[i]].reset(cf_handles_raw[i]);
    }

    sw.stop();
    spdlog::critical("RocksDB opened at path: {} with CFs, took {} µs",
                     dbname, sw.elapsedMicros());
}



void DBManager::insertRecords(int numRecords) {
    if (!db_) throw std::runtime_error("DB not open.");

    StopWatch sw;
    sw.start();

    std::vector<std::string> columns = {"phone", "mail", "address", "name", "surname"};

    rocksdb::WriteBatch batch;
    for (int i = 0; i < numRecords; ++i) {
        const std::string key = "key" + std::to_string(i);
        for (const auto& column : columns) {
            const std::string value = column + "_value" + std::to_string(i) + std::string(1000, 'a');
            auto handle = cf_handles_.at(column).get();
            batch.Put(handle, key, value);
        }

        if (i % 500000 == 0) {
            auto s = db_->Write(rocksdb::WriteOptions(), &batch);
            if (!s.ok()) throw std::runtime_error("Batch write failed: " + s.ToString());
            batch.Clear();
            spdlog::debug("Inserted {} records...", i);
        }
    }

    if (batch.Count() > 0) {
        auto s = db_->Write(rocksdb::WriteOptions(), &batch);
        if (!s.ok()) throw std::runtime_error("Final batch write failed: " + s.ToString());
    }

    db_->Flush(rocksdb::FlushOptions());

    sw.stop();
    spdlog::critical("Inserted {} records across CFs in {} µs.", numRecords, sw.elapsedMicros());

    db_->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
}

std::vector<std::string> DBManager::scanSSTFilesForColumn(const std::string& dbname, const std::string& column) {
    if (!db_) throw std::runtime_error("DB not open.");
    if (cf_handles_.find(column) == cf_handles_.end())
        throw std::runtime_error("Unknown Column Family: " + column);

    rocksdb::ColumnFamilyMetaData meta;
    db_->GetColumnFamilyMetaData(cf_handles_[column].get(), &meta);

    std::vector<std::string> sst_files;
    for (const auto& level : meta.levels) {
        for (const auto& file : level.files) {
            // skip "/" in filename (first character)
            sst_files.push_back(dbname + file.name);
        }
    }

    spdlog::info("Column {} has {} SST files.", column, sst_files.size());
    return sst_files;
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
        cf_handles_.clear();  // Automatically deletes handles
        db_.reset();
        spdlog::debug("DB closed with Column Families.");
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

// In DBManager class
bool DBManager::checkValueAcrossHierarchies( bloomTree& hierarchy1, const std::string& value1,
     bloomTree& hierarchy2, const std::string& value2) {
StopWatch sw;
sw.start();

auto candidates1 = hierarchy1.checkExistance(value1);
auto candidates2 = hierarchy2.checkExistance(value2);

std::unordered_set<std::string> setCandidates2(candidates2.begin(), candidates2.end());

// Find intersection between candidates
for (const auto& candidate : candidates1) {
if (setCandidates2.count(candidate)) {
// Confirm existence in both hierarchies
bool foundValue1 = ScanFileForValue(candidate, value1);
bool foundValue2 = ScanFileForValue(candidate, value2);

if (foundValue1 && foundValue2) {
sw.stop();
spdlog::critical("checkValueAcrossHierarchies found both values in {} µs.", sw.elapsedMicros());
return true;
}
}
}

sw.stop();
spdlog::critical("checkValueAcrossHierarchies did not find both values in {} µs.", sw.elapsedMicros());
return false;
}