#include "bloom_manager.hpp"

#include <rocksdb/sst_file_reader.h>
#include <spdlog/spdlog.h>

#include <future>
#include <vector>

#include "bloomTree.hpp"
#include "bloom_value.hpp"
#include "stopwatch.hpp"

// New separate method to process an SST file:
std::vector<Node*> BloomManager::processSSTFile(const std::string& sstFile,
                                                size_t partitionSize,
                                                size_t bloomSize,
                                                int numHashFunctions) {
    std::vector<Node*> partitions;
    rocksdb::Options options;
    rocksdb::SstFileReader reader(options);
    auto status = reader.Open(sstFile);
    if (!status.ok()) {
        spdlog::error("Cannot open SST file: {}", sstFile);
        return partitions;
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
            partitions.push_back(new Node(std::move(partitionBloom), sstFile, partitionStartKey, lastKey));
            partitionBloom = BloomFilter(bloomSize, numHashFunctions);
            currentCount = 0;
            firstEntry = true;
        }
    }

    if (currentCount > 0) {
        partitions.push_back(new Node(std::move(partitionBloom), sstFile, partitionStartKey, lastKey));
    }

    delete iter;
    return partitions;
}

BloomTree BloomManager::createPartitionedHierarchy(const std::vector<std::string>& sstFiles,
                                                   size_t partitionSize,
                                                   size_t bloomSize,
                                                   int numHashFunctions,
                                                   int branchingRatio) {
    StopWatch sw;
    sw.start();
    BloomTree hierarchy(branchingRatio, bloomSize, numHashFunctions);

    std::vector<std::future<std::vector<Node*>>> futures;
    for (const auto& sstFile : sstFiles) {
        futures.push_back(std::async(std::launch::async, &BloomManager::processSSTFile,
                                     this, sstFile, partitionSize, bloomSize, numHashFunctions));
    }

    std::vector<Node*> allLeafNodes;
    for (auto& fut : futures) {
        std::vector<Node*> nodes = fut.get();
        allLeafNodes.insert(allLeafNodes.end(), nodes.begin(), nodes.end());
    }

    hierarchy.leafNodes = std::move(allLeafNodes);

    hierarchy.buildTree();
    sw.stop();
    spdlog::info("Bloom hierarchy successfully built from partitions using parallel processing in {} µs.", sw.elapsedMicros());
    return hierarchy;
}
