#include "bloom_manager.hpp"
#include "bloom_value.hpp"
#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <stdexcept>
#include <spdlog/spdlog.h>


void BloomManager::createBloomValuesForSSTFiles(const std::vector<std::string>& sstFiles) {
    for (const auto& sstFile : sstFiles) {
        bloom_value filter;

        // Open SST file and populate the Bloom filter
        rocksdb::Options options;
        rocksdb::SstFileReader reader(options);
        auto status = reader.Open(sstFile);
        if (!status.ok()) {
            spdlog::error("Failed to open SSTable: {}", status.ToString());
            continue;
        }

        auto iter = reader.NewIterator(rocksdb::ReadOptions());
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            filter.insert(iter->value().ToString());
        }
        delete iter;

        // Save Bloom filter to file
        std::string bloomFile = sstFile + ".bloom";
        filter.saveToFile(bloomFile);
        spdlog::debug("Created Bloom filter for {}  -> {} \n", sstFile, bloomFile);
    }
}

void BloomManager::createHierarchy(const std::vector<std::string>& sstFiles, bloomTree& hierarchy) {
    for (const auto& sstFile : sstFiles) {
        std::string bloomFile = sstFile + ".bloom";
        bloom_value filter = filter.loadFromFile(bloomFile);
        hierarchy.createLeafLevel(filter, sstFile);
    }
    hierarchy.createTree();
    spdlog::debug("Bloom filter hierarchy created successfully.");
}

void BloomManager::createPartitionedHierarchy(const std::vector<std::pair<std::string, std::vector<Partition>>>& partitionedSSTFiles,
                                                bloomTree & hierarchy) {
    for (const auto & fileAndPartitions : partitionedSSTFiles) {
        const std::string & sstFile = fileAndPartitions.first;
        for (const auto & part : fileAndPartitions.second) {
            // Build an identifier string in the format:
            //    sstFileName[startKey,endKey]
            std::string id = sstFile + "[" + part.start_key + "," + (part.end_key.empty() ? "EOF" : part.end_key) + "]";
            hierarchy.createLeafLevel(part.bloom, id);
        }
    }
    hierarchy.createTree();
    spdlog::debug("Partitioned bloom filter hierarchy created successfully.");
}
