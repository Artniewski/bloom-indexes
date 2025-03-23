#include "db_manager.hpp"

#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table_properties.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <future>
#include <stdexcept>

#include "compaction_event_listener.hpp"
#include "stopwatch.hpp"

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
        if (i % 100000 == 0) {
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
            spdlog::info("Found '{}' in column '{}' in {} µs.", value, column, sw.elapsedMicros());
            return true;
        }
    }

    sw.stop();
    spdlog::info("Did NOT find '{}' in column '{}' after {} µs.", value, column, sw.elapsedMicros());
    return false;
}

bool DBManager::noBloomCheckRecordWithTwoColumns(const std::string& column1, const std::string& value1,
                                                 const std::string& column2, const std::string& value2) {
    StopWatch sw;

    if (!db_) {
        throw std::runtime_error("DB not open.");
    }

    auto cf_handle1 = cf_handles_.find(column1);
    auto cf_handle2 = cf_handles_.find(column2);

    if (cf_handle1 == cf_handles_.end() || cf_handle2 == cf_handles_.end()) {
        throw std::runtime_error("One or both Column Families not found.");
    }

    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = false;

    auto iter1 = std::unique_ptr<rocksdb::Iterator>(db_->NewIterator(readOptions, cf_handle1->second.get()));

    sw.start();
    for (iter1->SeekToFirst(); iter1->Valid(); iter1->Next()) {
        if (iter1->value().ToString() == value1) {
            std::string key = iter1->key().ToString();

            // Verify match in second column
            std::string value2_candidate;
            auto s = db_->Get(readOptions, cf_handle2->second.get(), key, &value2_candidate);

            if (s.ok() && value2_candidate == value2) {
                sw.stop();
                spdlog::info("Found matching record: [{}='{}', {}='{}'] in {} µs.",
                             column1, value1, column2, value2, sw.elapsedMicros());
                return true;
            }
        }
    }

    sw.stop();
    spdlog::info("No matching record found for [{}='{}', {}='{}'] after {} µs.",
                 column1, value1, column2, value2, sw.elapsedMicros());
    return false;
}

std::unordered_set<std::string> DBManager::scanFileForKeysWithValue(const std::string& filename, const std::string& value,
                                                                    const std::string& rangeStart, const std::string& rangeEnd) {
    std::unordered_set<std::string> matchingKeys;
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
            matchingKeys.insert(currentKey);
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

    sw.stop();
    spdlog::info("No matching record found for '{}' after {} µs.", value, sw.elapsedMicros());
    return false;
}

bool DBManager::findRecordInHierarchies(BloomTree& hierarchy1, const std::string& value1,
                                        BloomTree& hierarchy2, const std::string& value2) {
    StopWatch sw;
    sw.start();

    // Query first hierarchy to get candidates containing node
    auto candidates1 = hierarchy1.queryNodes(value1, "", "");
    // log candidates
    for (const auto& candidate : candidates1) {
        spdlog::info("Candidate found in the first hierarchy for '{}': {} to {} in {}.",
                     value1, candidate->startKey, candidate->endKey, candidate->filename);
    }
    if (candidates1.empty()) {
        spdlog::info("No candidates found in the first hierarchy for '{}'.", value1);
        return false;
    }
    // query second hierarchy for each candidate in the first hierarchy
    for (const auto& candidate1 : candidates1) {
        // log candidate start end key
        spdlog::info("Checking candidate1 start key: {} end key: {}", candidate1->startKey, candidate1->endKey);
        auto nodes = hierarchy2.queryNodes(value2, candidate1->startKey, candidate1->endKey);
        if (!nodes.empty()) {
            sw.stop();
            spdlog::info("Found matching record for '{}' and '{}' after {} µs.",
                         value1, value2, sw.elapsedMicros());
        }
        auto keys1 = scanFileForKeysWithValue(candidate1->filename, value1, candidate1->startKey, candidate1->endKey);
        // log keys
        for (const auto& key : keys1) {
            spdlog::info("Keys found in candidate1: {}", key);
        }
        spdlog::info("Keys found in candidate1: {}", keys1.size());
        for (const auto& node : nodes) {
            auto keys2 = scanFileForKeysWithValue(node->filename, value2, node->startKey, node->endKey);
            spdlog::info("Keys found in node: {}", keys2.size());

            // check if any key from keys2 is in keys1
            for (const auto& key : keys1) {
                if (keys2.find(key) != keys2.end()) {
                    sw.stop();
                    spdlog::info("[key check] Found matching record for '{}' and '{}' after {} µs.",
                                 value1, value2, sw.elapsedMicros());
                    spdlog::info("Keys match: {}", key);
                    return true;
                }
            }
        }
    }

    sw.stop();
    spdlog::info("No matching record found for '{}' and '{}' after {} µs.",
                 value1, value2, sw.elapsedMicros());
    return false;
}
