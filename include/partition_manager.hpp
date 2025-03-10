#ifndef PARTITION_MANAGER_HPP
#define PARTITION_MANAGER_HPP

#include <string>
#include <vector>
#include <utility>
#include "bloom_value.hpp"
#include <rocksdb/sst_file_reader.h>

enum class PartitioningMode {
    FixedSize,   
    BlockBased   
};

struct Partition {
    std::string start_key;    
    std::string end_key;      
    bloom_value bloom;        
};

/**
 * @brief Handles building partitions from an SST file and scanning partitions.
 */
class PartitionManager {
public:
    // Build partitions for a single SST file.
    std::vector<Partition> buildPartitions(const std::string & sstFile, PartitioningMode mode);

    // Build partitions for many SST files.
    std::vector<std::pair<std::string, std::vector<Partition>>>
    buildPartitionsForSSTFiles(const std::vector<std::string>& sstFiles, PartitioningMode mode);

    // Scans a single partition (between partitionStart and partitionEnd) for a value.
    bool scanPartitionForValue(const std::string & sstFile,
                               const std::string & partitionStart,
                               const std::string & partitionEnd,
                               const std::string & value);

    // Using a bloom hierarchy built from partitions, check for a value.
    // (This function assumes that a separate BloomManager has built the hierarchy.)
    bool checkValueInPartitionedHierarchy(const std::string & value, class bloomTree & hierarchy);
};

#endif // PARTITION_MANAGER_HPP
