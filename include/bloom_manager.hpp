#ifndef BLOOM_MANAGER_HPP
#define BLOOM_MANAGER_HPP

#include <string>
#include <vector>
#include <boost/asio/thread_pool.hpp>

#include "bloomTree.hpp"

extern boost::asio::thread_pool globalThreadPool;

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
