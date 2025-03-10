#ifndef BLOOM_MANAGER_HPP
#define BLOOM_MANAGER_HPP

#include <string>
#include <vector>
#include "bloomTree.hpp"
#include "partition_manager.hpp"   // For Partition

/**
 * @brief A class handling the creation and usage of Bloom filters from SST files.
 */
class BloomManager {
public:
    /**
     * @brief Create Bloom filters for the list of SST files.
     */
    void createBloomValuesForSSTFiles(const std::vector<std::string>& sstFiles);

    /**
     * @brief Create a Bloom filter hierarchy from the generated Bloom filters.
     */
    void createHierarchy(const std::vector<std::string>& sstFiles, bloomTree& hierarchy);

        // New method: build a hierarchy from partitions.
    // The input is a vector of pairs, each containing an SST file name and its partitions.
    void createPartitionedHierarchy(const std::vector<std::pair<std::string, std::vector<Partition>>>& partitionedSSTFiles,
                                    bloomTree & hierarchy);
};

#endif // BLOOM_MANAGER_HPP
