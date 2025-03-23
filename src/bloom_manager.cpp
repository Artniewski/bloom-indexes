#include "bloom_manager.hpp"

#include <rocksdb/sst_file_reader.h>
#include <spdlog/spdlog.h>

#include "bloom_value.hpp"
#include "bloomTree.hpp"

BloomTree BloomManager::createPartitionedHierarchy(const std::vector<std::string>& sstFiles,
                                              size_t partitionSize,
                                              size_t bloomSize,
                                              int numHashFunctions,
                                              int branchingRatio) {
    BloomTree hierarchy(branchingRatio, bloomSize, numHashFunctions);

    for (const auto& sstFile : sstFiles) {
        rocksdb::Options options;
        rocksdb::SstFileReader reader(options);
        auto status = reader.Open(sstFile);
        if (!status.ok()) {
            spdlog::error("Cannot open SST file: {}", sstFile);
            continue;
        }

        auto iter = reader.NewIterator(rocksdb::ReadOptions());

        size_t currentCount = 0;
        BloomFilter partitionBloom(bloomSize, numHashFunctions);
        std::string partitionStartKey;
        bool firstEntry = true;
        std::string lastKey;

        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            std::string key = iter->key().ToString();
            std::string value = iter->value().ToString();

            if (firstEntry) {
                partitionStartKey = key;
                firstEntry = false;
            }

            partitionBloom.insert(value);
            lastKey = key;
            currentCount++;

            if (currentCount >= partitionSize) {
                hierarchy.addLeafNode(std::move(partitionBloom), sstFile, partitionStartKey, lastKey);

                // Reset for next partition
                partitionBloom = BloomFilter(bloomSize, numHashFunctions);
                currentCount = 0;
                firstEntry = true;
            }
        }

        // Add remaining entries as a final partition
        if (currentCount > 0) {
            hierarchy.addLeafNode(std::move(partitionBloom), sstFile, partitionStartKey, lastKey);
        }

        delete iter;
    }

    hierarchy.buildTree();
    spdlog::info("Bloom hierarchy successfully built from partitions.");
    return hierarchy;
}
