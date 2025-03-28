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

   private:
    std::vector<Node*> processSSTFile(const std::string& sstFile,
                                      size_t partitionSize,
                                      size_t bloomSize,
                                      int numHashFunctions);
};

#endif  // BLOOM_MANAGER_HPP
