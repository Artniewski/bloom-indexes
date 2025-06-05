#include "db_manager.hpp"

#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table_properties.h>
#include <spdlog/spdlog.h>

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <filesystem>
#include <future>
#include <random>
#include <stdexcept>
#include <unordered_set>

#include "algorithm.hpp"
#include "stopwatch.hpp"

extern boost::asio::thread_pool globalThreadPool;

void DBManager::compactAllColumnFamilies(size_t numRecords) {
  if (!db_) throw std::runtime_error("DB not open");
  rocksdb::CompactRangeOptions opts;

  auto s_enable_del = db_->EnableFileDeletions(true);
  if (!s_enable_del.ok()) {
      spdlog::warn("Failed to ensure file deletions are enabled: {}. DB size may grow.", s_enable_del.ToString());
  }

  for (auto& kv : cf_handles_) {
    auto* handle = kv.second.get();
    auto s_flush = db_->Flush(rocksdb::FlushOptions(), handle);
    if (!s_flush.ok()) {
        spdlog::error("Flush failed for CF '{}': {}. Skipping compaction for this CF.", kv.first, s_flush.ToString());
        continue;
    }

    rocksdb::Slice begin, end;
    std::string endKeyStr;

    if (numRecords > 0) {
      begin = rocksdb::Slice();
      const std::string index_str = std::to_string(numRecords);
      const std::string prefixedIndex =
          std::string(20 - index_str.size(), '0') + index_str;
      endKeyStr = "key" + prefixedIndex;
      end = rocksdb::Slice(endKeyStr);
      
      spdlog::info("Starting ranged compaction for CF '{}' up to key '{}'", kv.first, endKeyStr);
      auto status = db_->CompactRange(opts, handle, &begin, &end);
      if (!status.ok()) {
        spdlog::error("Ranged compaction failed for CF '{}': {}", kv.first, status.ToString());
      } else {
        spdlog::debug("Ranged compaction succeeded for CF '{}'", kv.first);
      }
    } else {
      spdlog::info("Starting full compaction for CF '{}'", kv.first);
      auto status = db_->CompactRange(opts, handle, nullptr, nullptr);
      if (!status.ok()) {
        spdlog::error("Full compaction failed for CF '{}': {}", kv.first, status.ToString());
      } else {
        spdlog::info("Full compaction succeeded for CF '{}'", kv.first);
      }
    }
  }

  spdlog::info("Waiting for all background compactions to finish across the DB...");
  rocksdb::WaitForCompactOptions wco;

  auto s_wait = db_->WaitForCompact(wco);
  if (!s_wait.ok()) {
    spdlog::error("Error occurred while waiting for all compactions to finish: {}", s_wait.ToString());
  } else {
    spdlog::info("All background compactions finished successfully.");
  }
}

void DBManager::openDB(const std::string& dbname,
                       std::vector<std::string> columns) {
  StopWatch sw;
  sw.start();

  if (db_) {
    spdlog::warn("DB already open, closing before reopening.");
    closeDB();
  }

  rocksdb::DBOptions dbOptions;
  dbOptions.create_if_missing = true;
  dbOptions.create_missing_column_families = true;

  std::vector<std::string> cf_names = columns;
  cf_names.push_back("default");
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
  for (const auto& name : cf_names) {
    cf_descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
  }

  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_raw;
  rocksdb::DB* rawDbPtr = nullptr;

  auto status = rocksdb::DB::Open(dbOptions, dbname, cf_descriptors,
                                  &cf_handles_raw, &rawDbPtr);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open DB with Column Families: " +
                             status.ToString());
  }

  db_.reset(rawDbPtr);

  // Store ColumnFamilyHandles in a map for easy access
  cf_handles_.clear();
  for (size_t i = 0; i < cf_names.size(); ++i) {
    cf_handles_[cf_names[i]].reset(cf_handles_raw[i]);
  }

  sw.stop();
  spdlog::critical("RocksDB opened at path: {} with CFs, took {} µs", dbname,
                   sw.elapsedMicros());
}

void DBManager::insertRecords(int numRecords,
                              std::vector<std::string> columns) {
  if (!db_) throw std::runtime_error("DB not open.");

  StopWatch sw;
  sw.start();
  spdlog::info("Inserting {} records across {} CFs...", numRecords,
               columns.size());

  rocksdb::WriteBatch batch;
  for (int i = 1; i <= numRecords; ++i) {
    // prefix index with 0s to ensure lexicographical order based on numRecords
    // size
    const std::string index = std::to_string(i);
    const std::string prefixedIndex =
        std::string(20 - index.size(), '0') + std::to_string(i);

    const std::string key = "key" + prefixedIndex;
    for (const auto& column : columns) {
      const std::string value = column + "_value" + index;
      auto handle = cf_handles_.at(column).get();
      batch.Put(handle, key, value);
    }
    if (i % 1000000 == 0) {
      auto s = db_->Write(rocksdb::WriteOptions(), &batch);
      if (!s.ok())
        throw std::runtime_error("Batch write failed: " + s.ToString());
      batch.Clear();
      spdlog::debug("Inserted {} records...", i);
    }
  }

  if (batch.Count() > 0) {
    auto s = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!s.ok())
      throw std::runtime_error("Final batch write failed: " + s.ToString());
  }

  for (const auto& column : columns) {
    auto handle = cf_handles_.at(column).get();
    auto s = db_->Flush(rocksdb::FlushOptions(), handle);
    if (!s.ok()) throw std::runtime_error("Flush failed: " + s.ToString());
  }

  sw.stop();
  spdlog::critical("Inserted {} records across CFs in {} µs.", numRecords,
                   sw.elapsedMicros());
}

void DBManager::insertRecordsWithSearchTargets(
    int numRecords, const std::vector<std::string>& columns,
    const std::unordered_set<int>& targetIndices) {
  if (!db_) throw std::runtime_error("DB not open.");

  StopWatch sw;
  sw.start();
  spdlog::info("Inserting {} records across {} CFs... with {} search targets",
               numRecords, columns.size(), targetIndices.size());

  rocksdb::WriteBatch batch;
  for (int i = 1; i <= numRecords; ++i) {
    bool isTarget = targetIndices.find(i) != targetIndices.end();

    // prefix index with 0s to ensure lexicographical order based on numRecords
    // size
    const std::string index = std::to_string(i);
    const std::string prefixedIndex =
        std::string(20 - index.size(), '0') + std::to_string(i);

    const std::string key = "key" + prefixedIndex;
    for (const auto& column : columns) {
      auto handle = cf_handles_.at(column).get();
      std::string value;

      if (isTarget) {
        value = column + "_target";
        spdlog::debug("Creating target value: {} for key: {}", value, key);
      } else {
        value = column + "_value" + index;
      }

      batch.Put(handle, key, value);
    }
    if (i % 1000000 == 0) {
      auto s = db_->Write(rocksdb::WriteOptions(), &batch);
      if (!s.ok())
        throw std::runtime_error("Batch write failed: " + s.ToString());
      batch.Clear();
      spdlog::debug("Inserted {} records...", i);
    }
  }

  if (batch.Count() > 0) {
    auto s = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!s.ok())
      throw std::runtime_error("Final batch write failed: " + s.ToString());
  }

  for (const auto& column : columns) {
    auto handle = cf_handles_.at(column).get();
    auto s = db_->Flush(rocksdb::FlushOptions(), handle);
    if (!s.ok()) throw std::runtime_error("Flush failed: " + s.ToString());
  }

  sw.stop();
  spdlog::critical("Inserted {} records across CFs in {} µs.", numRecords,
                   sw.elapsedMicros());
}

std::vector<std::string> DBManager::scanSSTFilesForColumn(
    const std::string& dbname, const std::string& column) {
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
  spdlog::critical("checkValueWithoutBloomFilters setup took {} µs.",
                   sw.elapsedMicros());

  sw.start();
  // Create an iterator to scan all K-V pairs in the open DB
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(readOptions));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (iter->value().ToString() == value) {
      sw.stop();
      spdlog::critical("checkValueWithoutBloomFilters took {} µs (found).",
                       sw.elapsedMicros());
      return true;
    }
  }
  sw.stop();
  spdlog::critical("checkValueWithoutBloomFilters took {} µs (not found).",
                   sw.elapsedMicros());
  return false;
}

rocksdb::Status DBManager::closeDB() {
  StopWatch sw;
  sw.start();

  if (db_) {
    cf_handles_.clear();  // Automatically deletes handles
    db_.reset();
    spdlog::debug("DB closed with Column Families.");
  }

  sw.stop();
  spdlog::critical("closeDB took {} µs.", sw.elapsedMicros());
  return rocksdb::Status::OK();
}

bool DBManager::ScanFileForValue(const std::string& filename,
                                 const std::string& value) {
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

  auto iter =
      std::unique_ptr<rocksdb::Iterator>(reader.NewIterator(readOptions));

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

bool DBManager::noBloomcheckValueInColumn(const std::string& column,
                                          const std::string& value) {
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

  auto iter = std::unique_ptr<rocksdb::Iterator>(
      db_->NewIterator(readOptions, cf_it->second.get()));

  sw.start();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (iter->value().ToString() == value) {
      sw.stop();
      // spdlog::info("Found '{}...' in column '{}' in {} µs.", value.substr(0,
      // 30), column, sw.elapsedMicros());
      return true;
    }
  }

  sw.stop();
  spdlog::info("Did NOT find '{}...' in column '{}' after {} µs.",
               value.substr(0, 30), column, sw.elapsedMicros());
  return false;
}

std::vector<std::string> DBManager::scanForRecordsInColumns(
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values) {
  if (columns.size() != values.size() || columns.empty()) {
    throw std::runtime_error(
        "Number of columns and values must be equal and non-empty.");
  }

  StopWatch sw;
  sw.start();

  std::vector<std::string> matchingKeys;

  // Use the first column as the base for scanning.
  auto baseIt = cf_handles_.find(columns[0]);
  if (baseIt == cf_handles_.end()) {
    throw std::runtime_error("Column Family not found for base column: " +
                             columns[0]);
  }

  rocksdb::ReadOptions readOptions;
  readOptions.fill_cache = false;

  // Create an iterator for the base column and scan the entire key range.
  std::unique_ptr<rocksdb::Iterator> iter(
      db_->NewIterator(readOptions, baseIt->second.get()));
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
      auto status =
          db_->Get(readOptions, cfIt->second.get(), key, &candidateValue);
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
  spdlog::info(
      "Scanned entire DB for {} columns in {} µs, found {} matching keys.",
      columns.size(), sw.elapsedMicros(), matchingKeys.size());

  return matchingKeys;
}

std::vector<std::string> DBManager::scanFileForKeysWithValue(
    const std::string& filename, const std::string& value,
    const std::string& rangeStart, const std::string& rangeEnd) {
  std::vector<std::string> matchingKeys;
  rocksdb::Options options;
  options.env = rocksdb::Env::Default();

  rocksdb::SstFileReader reader(options);
  auto status = reader.Open(filename);
  if (!status.ok()) {
    spdlog::error("Failed to open SSTable '{}': {}", filename,
                  status.ToString());
    return {};
  }

  rocksdb::ReadOptions readOptions;
  readOptions.fill_cache = false;

  auto iter =
      std::unique_ptr<rocksdb::Iterator>(reader.NewIterator(readOptions));
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

bool DBManager::findRecordInHierarchy(BloomTree& hierarchy,
                                      const std::string& value,
                                      const std::string& startKey,
                                      const std::string& endKey) {
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
    futures.emplace_back(
        std::async(std::launch::async, [this, &candidate, &value, &found]() {
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
      if (it->wait_for(std::chrono::milliseconds(0)) ==
          std::future_status::ready) {
        if (it->get()) {
          spdlog::debug("Value truly found in one of the files.");
          sw.stop();
          spdlog::critical("checkValueInHierarchy took {} µs.",
                           sw.elapsedMicros());
          return true;
        }
        it = futures.erase(it);
      } else {
        ++it;
      }
    }
  }

  sw.stop();
  spdlog::info("No matching record found for '{}' after {} µs.", value,
               sw.elapsedMicros());
  return false;
}

std::vector<std::string> DBManager::findUsingSingleHierarchy(
    BloomTree& hierarchy, const std::vector<std::string>& columns,
    const std::vector<std::string>& values) {
  if (columns.size() != values.size() || columns.empty()) {
    throw std::runtime_error(
        "Number of columns and values must be equal and non-empty.");
  }

  // StopWatch sw;
  // sw.start();

  std::vector<const Node*> candidates = hierarchy.queryNodes(values[0], "", "");
  if (candidates.empty()) {
    spdlog::info("No candidates found in the hierarchy for '{}'.", values[0]);
    return {};
  }

  std::vector<std::string> allKeys;

  // Count SSTable checks
  extern std::atomic<size_t> gSSTCheckCount;
  gSSTCheckCount.store(0);
  gSSTCheckCount += candidates.size();  // Increment by the number of SST files
                                        // we are about to process.
  // spdlog::info(
  //     "SSTables to check based on hierarchy for primary column: {}, current "
  //     "total checked: {}",
  //     candidates.size(), gSSTCheckCount.load());

  std::vector<std::future<std::vector<std::string>>> sst_scan_futures;
  sst_scan_futures.reserve(candidates.size());

  for (const auto* candidate_node :
       candidates) {  // Changed to const auto* as queryNodes returns
                      // vector<const Node*>
    std::promise<std::vector<std::string>> promise_sst_keys;
    sst_scan_futures.emplace_back(promise_sst_keys.get_future());

    // Capture necessary data by value for the lambda
    std::string filename = candidate_node->filename;
    std::string value_to_scan = values[0];
    std::string start_key = candidate_node->startKey;
    std::string end_key = candidate_node->endKey;

    boost::asio::post(globalThreadPool,
                      [this, filename, value_to_scan, start_key, end_key,
                       p_sst_keys = std::move(promise_sst_keys)]() mutable {
                        try {
                          auto keys = scanFileForKeysWithValue(
                              filename, value_to_scan, start_key, end_key);
                          p_sst_keys.set_value(keys);
                        } catch (...) {
                          try {
                            p_sst_keys.set_exception(std::current_exception());
                          } catch (...) {
                          }
                        }
                      });
  }

  allKeys.reserve(100);
  for (auto& fut_sst_keys : sst_scan_futures) {
    try {
      std::vector<std::string> keys_from_sst = fut_sst_keys.get();
      allKeys.insert(allKeys.end(), keys_from_sst.begin(), keys_from_sst.end());
    } catch (const std::exception& e) {
      spdlog::error("Exception during parallel SST scan: {}", e.what());
    }
  }

  // spdlog::info("Total keys collected from primary column scan: {}",
  //              allKeys.size());

  std::vector<std::future<std::string>> futures;
  futures.reserve(allKeys.size());  // Reserve space for futures

  for (const auto& key : allKeys) {
    // spdlog::info("Checking key: {}", key.substr(0, 30));
    std::promise<std::string> promise;
    futures.emplace_back(promise.get_future());

    boost::asio::post(globalThreadPool, [this, key, captured_columns = columns,
                                         captured_values = values,
                                         p = std::move(promise)]() mutable {
      rocksdb::ReadOptions readOptionsLocal;
      readOptionsLocal.fill_cache = false;
      bool all_columns_match = true;

      if (captured_columns.size() > 1) {
        for (size_t i = 1; i < captured_columns.size(); ++i) {
          auto cf_it = cf_handles_.find(captured_columns[i]);
          if (cf_it == cf_handles_.end()) {
            spdlog::warn(
                "Column Family {} not found for key {} during Get operation in "
                "findUsingSingleHierarchy.",
                captured_columns[i], key);
            all_columns_match = false;
            break;
          }
          rocksdb::ColumnFamilyHandle* handle = cf_it->second.get();
          std::string actual_value;
          auto status = db_->Get(readOptionsLocal, handle, key, &actual_value);

          if (!status.ok()) {
            if (status.IsNotFound()) {
              spdlog::debug(
                  "Key {} not found in column {} during Get operation.", key,
                  captured_columns[i]);
            } else {
              spdlog::warn("RocksDB Get failed for key {} in column {}: {}",
                           key, captured_columns[i], status.ToString());
            }
            all_columns_match = false;
            break;
          }
          if (actual_value != captured_values[i]) {
            all_columns_match = false;
            break;
          }
        }
      }
      // If all_columns_match is still true, it means:
      // 1. There was only one column (already verified by
      // scanFileForKeysWithValue), OR
      // 2. All subsequent columns also matched their respective values for the
      // given key.
      if (all_columns_match) {
        p.set_value(key);
      } else {
        p.set_value(std::string());  // Indicate no match with an empty string
      }
    });
  }

  // Wait for all asynchronous tasks to finish and collect matching keys.
  std::vector<std::string> matchingKeys;
  for (auto& fut : futures) {
    std::string res = fut.get();
    if (!res.empty()) {
      matchingKeys.push_back(res);
    }
  }

  // sw.stop();
  // spdlog::critical("Single hierarchy check took {} µs, found {} matching keys.",
  //                  sw.elapsedMicros(), matchingKeys.size());
  // spdlog::info(
  //     "Bloom filters checked: {} (total), {} (leaves only), SSTables checked: "
  //     "{}",
  //     gBloomCheckCount.load(), gLeafBloomCheckCount.load(),
  //     gSSTCheckCount.load());
  return matchingKeys;
}

std::string DBManager::getValue(const std::string& column_family_name,
                                const std::string& key) {
  rocksdb::PinnableSlice value;
  rocksdb::ColumnFamilyHandle* cf_handle =
      getColumnFamilyHandle(column_family_name);
  if (!cf_handle) {
    throw std::runtime_error("Column family not found: " + column_family_name);
  }
  rocksdb::ReadOptions read_options;
  rocksdb::Status status =
      db_->Get(read_options, cf_handle, rocksdb::Slice(key), &value);
  if (status.ok()) {
    return value.ToString();
  }
  return "";  // Or throw an exception / return status
}

rocksdb::ColumnFamilyHandle* DBManager::getColumnFamilyHandle(
    const std::string& column_family_name) {
  auto it = cf_handles_.find(column_family_name);
  if (it != cf_handles_.end()) {
    return it->second.get();
  }
  return nullptr;  // Or throw an exception
}

rocksdb::Status DBManager::applyModifications(
    const std::vector<std::tuple<std::string, std::string, std::string>>&
        modifications, size_t numRecords) {
  if (!db_) return rocksdb::Status::InvalidArgument("DB not open");

  rocksdb::WriteOptions write_options;
  for (const auto& mod : modifications) {
    const std::string& key = std::get<0>(mod);
    const std::string& column_name = std::get<1>(mod);
    const std::string& value = std::get<2>(mod);

    auto handle = cf_handles_.at(column_name).get();
    if (!handle) {
      spdlog::error(
          "ApplyModifications: Column family '{}' not found for key '{}'. "
          "Skipping.",
          column_name, key);
      continue;
    }
    rocksdb::Status s = db_->Put(write_options, handle, key, value);
    if (!s.ok()) {
      spdlog::error(
          "ApplyModifications: Failed to Put key '{}' in column '{}': {}", key,
          column_name, s.ToString());
      return s;
    }
  }
  compactAllColumnFamilies(numRecords);
  return rocksdb::Status::OK();
}

rocksdb::Status DBManager::revertModifications(
    const std::vector<std::tuple<std::string, std::string, std::string>>&
        reversions, size_t numRecords) {
  if (!db_) return rocksdb::Status::InvalidArgument("DB not open");

  rocksdb::WriteOptions write_options;
  for (const auto& rev : reversions) {
    const std::string& key = std::get<0>(rev);
    const std::string& column_name = std::get<1>(rev);
    const std::string& value = std::get<2>(rev);

    auto handle = cf_handles_.at(column_name).get();
    if (!handle) {
      spdlog::error(
          "RevertModifications: Column family '{}' not found for key '{}'. "
          "Skipping.",
          column_name, key);
      continue;
    }
    rocksdb::Status s = db_->Put(write_options, handle, key, value);
    if (!s.ok()) {
      spdlog::error(
          "RevertModifications: Failed to Put key '{}' in column '{}': {}", key,
          column_name, s.ToString());
      return s;
    }
  }
  compactAllColumnFamilies(numRecords);
  return rocksdb::Status::OK();
}
