#include "partition_manager.hpp"
#include "bloom_value.hpp"
#include "bloomTree.hpp"  // for using the hierarchy
#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/table_properties.h>
#include <rocksdb/iterator.h>
#include <filesystem>
#include <stdexcept>
#include <spdlog/spdlog.h>
#include <future>
#include "stopwatch.hpp"

std::vector<Partition> PartitionManager::buildPartitions(const std::string & sstFile, PartitioningMode mode) {
    std::vector<Partition> partitions;
    
    rocksdb::Options options;
    options.env = rocksdb::Env::Default();
    rocksdb::SstFileReader reader(options);
    auto status = reader.Open(sstFile);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open SST file " + sstFile + ": " + status.ToString());
    }
    
    auto table_properties = reader.GetTableProperties();
    uint64_t partitionSize = 100000;  // Default fixed size.
    if (mode == PartitioningMode::BlockBased && table_properties) {
        uint64_t entriesPerBlock = table_properties->num_entries / table_properties->num_data_blocks;
        uint64_t blocksPerPartition = 10000;  // Adjust as needed.
        partitionSize = entriesPerBlock * blocksPerPartition;
    }
    
    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = false;
    readOptions.verify_checksums = true;
    auto iter = std::unique_ptr<rocksdb::Iterator>(reader.NewIterator(readOptions));
    
    uint64_t count = 0;
    bloom_value currentBloom;
    std::string partitionStartKey;
    bool firstEntry = true;
    
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::string key = iter->key().ToString();
        std::string val = iter->value().ToString();
        if (firstEntry) {
            partitionStartKey = key;
            firstEntry = false;
        }
        currentBloom.insert(val);
        ++count;
        if (count >= partitionSize) {
            Partition part;
            part.start_key = partitionStartKey;
            part.end_key = key;
            part.bloom = currentBloom;
            partitions.push_back(std::move(part));
            count = 0;
            currentBloom = bloom_value();
            firstEntry = true;
        }
    }
    if (count > 0) {
        Partition part;
        part.start_key = partitionStartKey;
        part.end_key = "";
        part.bloom = currentBloom;
        partitions.push_back(std::move(part));
    }
    spdlog::critical("buildPartitions: Created {} partitions for SST file {} using mode {}.",
                     partitions.size(), sstFile, mode == PartitioningMode::BlockBased ? "BlockBased" : "FixedSize");
    return partitions;
}

std::vector<std::pair<std::string, std::vector<Partition>>>
PartitionManager::buildPartitionsForSSTFiles(const std::vector<std::string>& sstFiles,
                                             PartitioningMode mode) {
    std::vector<std::pair<std::string, std::vector<Partition>>> result;
    for (const auto & sstFile : sstFiles) {
        auto parts = buildPartitions(sstFile, mode);
        result.push_back({ sstFile, parts });
        //save as files
        for (int i = 0; i < parts.size(); i++) {
            std::string bloomFile = sstFile + ".bloom" + std::to_string(i);
            parts[i].bloom.saveToFile(bloomFile);
        }
    }
    return result;
}

bool PartitionManager::scanPartitionForValue(const std::string & sstFile,
                                               const std::string & partitionStart,
                                               const std::string & partitionEnd,
                                               const std::string & value) {
    rocksdb::Options options;
    options.env = rocksdb::Env::Default();
    rocksdb::SstFileReader reader(options);
    auto status = reader.Open(sstFile);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open SST file " + sstFile + " for partition scanning: " + status.ToString());
    }
    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = false;
    readOptions.verify_checksums = true;
    auto iter = std::unique_ptr<rocksdb::Iterator>(reader.NewIterator(readOptions));
    
    iter->Seek(partitionStart);
    while (iter->Valid()) {
        std::string curKey = iter->key().ToString();
        if (!partitionEnd.empty() && curKey > partitionEnd)
            break;
        if (iter->value().ToString() == value) {
            spdlog::critical("scanPartitionForValue: Found value \"{}\" in partition starting at key \"{}\".", value, partitionStart);
            return true;
        }
        iter->Next();
    }
    return false;
}

bool PartitionManager::checkValueInPartitionedHierarchy(const std::string & value, bloomTree & hierarchy) {
    StopWatch sw;
    sw.start();
    
    // Query the hierarchy. The returned vector contains candidate leaf identifiers.
    auto candidates = hierarchy.checkExistance(value);
    if (candidates.empty()) {
        spdlog::debug("Partitioned hierarchy indicates value \"{}\" not present in any partition.", value);
        sw.stop();
        spdlog::critical("checkValueInPartitionedHierarchy took {} µs.", sw.elapsedMicros());
        return false;
    }
    
    spdlog::debug("Value \"{}\" might exist in the following partitions:", value);
    for (const auto & candidate : candidates) {
        spdlog::debug("  - {}", candidate);
    }
    
    // For each candidate, parse the identifier and scan that partition.
    // Expected format:  sstFileName[startKey,endKey]
    std::vector<std::future<bool>> futures;
    std::atomic<bool> found{false};
    
    for (const auto & candidate : candidates) {
        futures.emplace_back(std::async(std::launch::async, [this, &candidate, &value, &found]() {
            // Parse the candidate string.
            auto pos1 = candidate.find('[');
            auto pos2 = candidate.find(',', pos1);
            auto pos3 = candidate.find(']', pos2);
            if (pos1 == std::string::npos || pos2 == std::string::npos || pos3 == std::string::npos) {
                spdlog::error("Candidate identifier '{}' is in an unexpected format.", candidate);
                return false;
            }
            std::string sstFile = candidate.substr(0, pos1);
            std::string startKey = candidate.substr(pos1 + 1, pos2 - pos1 - 1);
            std::string endKey = candidate.substr(pos2 + 1, pos3 - pos2 - 1);
            if (endKey == "EOF") {
                endKey = "";
            }
            
            bool result = scanPartitionForValue(sstFile, startKey, endKey, value);
            if (result) {
                found.store(true);
            }
            return result;
        }));
    }
    
    // Wait for futures; if any finds the value, return true.
    while (!futures.empty()) {
        for (auto it = futures.begin(); it != futures.end(); ) {
            if (it->wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
                if (it->get()) {
                    spdlog::debug("Value \"{}\" found in one of the candidate partitions.", value);
                    sw.stop();
                    spdlog::critical("checkValueInPartitionedHierarchy took {} µs.", sw.elapsedMicros());
                    return true;
                }
                it = futures.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    spdlog::debug("Value \"{}\" was not found in any candidate partition.", value);
    sw.stop();
    spdlog::critical("checkValueInPartitionedHierarchy took {} µs.", sw.elapsedMicros());
    return false;
}