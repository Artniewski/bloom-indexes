#ifndef BLOOM_MANAGER_HPP
#define BLOOM_MANAGER_HPP

#include <string>
#include <vector>

#include "bloomTree.hpp"

class BloomManager {
   public:
    BloomTree createPartitionedHierarchy(const std::vector<std::string>& sstFiles,
                                    size_t partitionSize,
                                    size_t bloomSize,
                                    int numHashFunctions,
                                    int branchingRatio);
};

#endif  // BLOOM_MANAGER_HPP
