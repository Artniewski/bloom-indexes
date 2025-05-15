#include "db_manager.hpp"

#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table_properties.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <future>
#include <stdexcept>
#include <unordered_set>

#include "compaction_event_listener.hpp"
#include "stopwatch.hpp"

void DBManager::compactAllColumnFamilies() {
    if (!db_) throw std::runtime_error("DB not open");
    rocksdb::CompactRangeOptions opts;
    for (auto& kv : cf_handles_) {
        auto* handle = kv.second.get();
        auto status = db_->CompactRange(opts, handle, /*begin=*/ nullptr, /*end=*/ nullptr);
        if (!status.ok()) {
            spdlog::error("Compaction failed for CF '{}': {}", kv.first, status.ToString());
        } else {
            spdlog::info("Compaction succeeded for CF '{}'", kv.first);
        }
    }
    auto s = db_->EnableFileDeletions(true);
    assert(s.ok());
}

void DBManager::openDB(const std::string& dbname, bool withListener, std::vector<std::string> columns) {
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

    std::vector<std::string> cf_names = columns;
    cf_names.push_back("default");
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

void DBManager::insertRecords(int numRecords, std::vector<std::string> columns) {
    if (!db_) throw std::runtime_error("DB not open.");

    StopWatch sw;
    sw.start();
    spdlog::info("Inserting {} records across {} CFs...", numRecords, columns.size());

    rocksdb::WriteBatch batch;
    for (int i = 1; i <= numRecords; ++i) {
        // prefix index with 0s to ensure lexicographical order based on numRecords size
        const std::string index = std::to_string(i);
        const std::string prefixedIndex = std::string(20 - index.size(), '0') + std::to_string(i);

        const std::string key = "key" + prefixedIndex;
        for (const auto& column : columns) {
            const std::string value = column + "_value" + index + std::string(1000, 'a');
            auto handle = cf_handles_.at(column).get();
            batch.Put(handle, key, value);
        }
        if (i % 1000000 == 0) {
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

    for (const auto& column : columns) {
        auto handle = cf_handles_.at(column).get();
        auto s = db_->Flush(rocksdb::FlushOptions(), handle);
        if (!s.ok()) throw std::runtime_error("Flush failed: " + s.ToString());
    }

    sw.stop();
    spdlog::critical("Inserted {} records across CFs in {} µs.", numRecords, sw.elapsedMicros());
}

void DBManager::insertRecordsWithSearchTargets(int numRecords, const std::vector<std::string>& columns, int targetCount, std::string searchPattern) {
    if (!db_) throw std::runtime_error("DB not open.");

    StopWatch sw;
    sw.start();
    spdlog::info("Inserting {} records across {} CFs... with {} search targets", numRecords, columns.size(), targetCount);

    int targetModulo = numRecords / targetCount;

    rocksdb::WriteBatch batch;
    for (int i = 1; i <= numRecords; ++i) {

        bool isTarget = (i % targetModulo == 0);
        // prefix index with 0s to ensure lexicographical order based on numRecords size
        const std::string index = std::to_string(i);
        const std::string prefixedIndex = std::string(20 - index.size(), '0') + std::to_string(i);

        const std::string key = "key" + prefixedIndex;
        for (const auto& column : columns) {
            auto handle = cf_handles_.at(column).get();
            const std::string value = column + "_value" + index + std::string(1000, 'a');
            if(isTarget){
                batch.Put(handle, key, searchPattern);
            } else {
                batch.Put(handle, key, value);
            }
        }
        if (i % 1000000 == 0) {
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

    for (const auto& column : columns) {
        auto handle = cf_handles_.at(column).get();
        auto s = db_->Flush(rocksdb::FlushOptions(), handle);
        if (!s.ok()) throw std::runtime_error("Flush failed: " + s.ToString());
    }

    sw.stop();
    spdlog::critical("Inserted {} records across CFs in {} µs.", numRecords, sw.elapsedMicros());
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
            sst_files.push_back(dbname + file.name);
        }
    }

    spdlog::info("Column {} has {} SST files.", column, sst_files.size());
    return sst_files;
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

bool DBManager::noBloomcheckValueInColumn(const std::string& column, const std::string& value) {
    StopWatch sw;

    if (!db_) {
        throw std::runtime_error("DB not open.");
    }

    auto cf_it = cf_handles_.find(column);
    if (cf_it == cf_handles_.end()) {
        throw std::runtime_error("Column Family not found: " + column);
    }

    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = false;

    auto iter = std::unique_ptr<rocksdb::Iterator>(db_->NewIterator(readOptions, cf_it->second.get()));

    sw.start();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (iter->value().ToString() == value) {
            sw.stop();
            // spdlog::info("Found '{}...' in column '{}' in {} µs.", value.substr(0, 30), column, sw.elapsedMicros());
            return true;
        }
    }

    sw.stop();
    spdlog::info("Did NOT find '{}...' in column '{}' after {} µs.", value.substr(0, 30), column, sw.elapsedMicros());
    return false;
}

std::vector<std::string> DBManager::scanForRecordsInColumns(
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values) {
    if (columns.size() != values.size() || columns.empty()) {
        throw std::runtime_error("Number of columns and values must be equal and non-empty.");
    }

    StopWatch sw;
    sw.start();

    std::vector<std::string> matchingKeys;

    // Use the first column as the base for scanning.
    auto baseIt = cf_handles_.find(columns[0]);
    if (baseIt == cf_handles_.end()) {
        throw std::runtime_error("Column Family not found for base column: " + columns[0]);
    }

    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = false;

    // Create an iterator for the base column and scan the entire key range.
    std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(readOptions, baseIt->second.get()));
    iter->SeekToFirst();

    for (; iter->Valid(); iter->Next()) {
        std::string key = iter->key().ToString();
        bool allMatch = true;
        // For each column, get the value associated with the same key.
        for (size_t i = 0; i < columns.size(); ++i) {
            auto cfIt = cf_handles_.find(columns[i]);
            if (cfIt == cf_handles_.end()) {
                allMatch = false;
                break;
            }
            std::string candidateValue;
            auto status = db_->Get(readOptions, cfIt->second.get(), key, &candidateValue);
            if (!status.ok() || candidateValue != values[i]) {
                allMatch = false;
                break;
            }
        }
        if (allMatch) {
            matchingKeys.push_back(key);
        }
    }

    sw.stop();
    spdlog::info("Scanned entire DB for {} columns in {} µs, found {} matching keys.",
                 columns.size(), sw.elapsedMicros(), matchingKeys.size());

    return matchingKeys;
}

std::vector<std::string> DBManager::scanFileForKeysWithValue(const std::string& filename, const std::string& value,
                                                             const std::string& rangeStart, const std::string& rangeEnd) {
    std::vector<std::string> matchingKeys;
    rocksdb::Options options;
    options.env = rocksdb::Env::Default();

    rocksdb::SstFileReader reader(options);
    auto status = reader.Open(filename);
    if (!status.ok()) {
        spdlog::error("Failed to open SSTable '{}': {}", filename, status.ToString());
        return {};
    }

    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = false;

    auto iter = std::unique_ptr<rocksdb::Iterator>(reader.NewIterator(readOptions));
    if (!rangeStart.empty()) {
        iter->Seek(rangeStart);
    } else {
        iter->SeekToFirst();
    }

    while (iter->Valid()) {
        std::string currentKey = iter->key().ToString();
        if (!rangeEnd.empty() && currentKey > rangeEnd) break;

        if (iter->value().ToString() == value) {
            matchingKeys.push_back(currentKey);
        }
        iter->Next();
    }

    return matchingKeys;
}

bool DBManager::findRecordInHierarchy(BloomTree& hierarchy, const std::string& value,
                                      const std::string& startKey, const std::string& endKey) {
    StopWatch sw;
    sw.start();

    auto candidates = hierarchy.query(value, startKey, endKey);
    if (candidates.empty()) {
        spdlog::info("No candidates found in the hierarchy for '{}'.", value);
        return false;
    }

    std::vector<std::future<bool>> futures;
    std::atomic<bool> found{false};

    for (const auto& candidate : candidates) {
        spdlog::info("Checking candidate: {} ", candidate);
        futures.emplace_back(std::async(std::launch::async, [this, &candidate, &value, &found]() {
            if (found.load()) {
                return false;
            }
            bool result = ScanFileForValue(candidate, value);
            if (result) {
                found.store(true);
            }
            return result;
        }));
    }

    while (!futures.empty()) {
        for (auto it = futures.begin(); it != futures.end();) {
            if (it->wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
                if (it->get()) {
                    spdlog::debug("Value truly found in one of the files.");
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

    sw.stop();
    spdlog::info("No matching record found for '{}' after {} µs.", value, sw.elapsedMicros());
    return false;
}

std::vector<std::string> DBManager::findUsingSingleHierarchy(BloomTree& hierarchy,
                                                             const std::vector<std::string>& columns,
                                                             const std::vector<std::string>& values) {
    if (columns.size() != values.size() || columns.empty()) {
        throw std::runtime_error("Number of columns and values must be equal and non-empty.");
    }

    StopWatch sw;
    sw.start();

    std::vector<const Node*> candidates = hierarchy.queryNodes(values[0], "", "");
    if (candidates.empty()) {
        spdlog::info("No candidates found in the hierarchy for '{}'.", values[0]);
        return {};
    }

    std::vector<std::string> allKeys;
    for (const auto& candidate : candidates) {
        auto keys = scanFileForKeysWithValue(candidate->filename, values[0], candidate->startKey, candidate->endKey);
        allKeys.insert(allKeys.end(), keys.begin(), keys.end());
    }

    std::vector<std::future<std::string>> futures;
    for (const auto& key : allKeys) {
        spdlog::info("Checking key: {}", key.substr(0, 30));
        futures.emplace_back(std::async(std::launch::async, [this, key, &columns, &values]() -> std::string {
            bool result = true;
            for (size_t i = 0; i < columns.size(); ++i) {
                if (!noBloomcheckValueInColumn(columns[i], values[i])) {
                    result = false;
                    break;
                }
            }
            return result ? key : std::string();
        }));
    }

    // Wait for all asynchronous tasks to finish and collect matching keys.
    std::vector<std::string> matchingKeys;
    for (auto& fut : futures) {
        std::string res = fut.get();
        if (!res.empty()) {
            matchingKeys.push_back(res);
        }
    }

    sw.stop();
    spdlog::critical("Single hierarchy check took {} µs, found {} matching keys.", sw.elapsedMicros(), matchingKeys.size());
    return matchingKeys;
}
