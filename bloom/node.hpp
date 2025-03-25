#pragma once
#include <memory>
#include <string>
#include <vector>

#include "bloom_value.hpp"

class Node {
   public:
    std::vector<Node*> children;
    BloomFilter bloom;
    std::string filename;
    std::string startKey;
    std::string endKey;

    Node(BloomFilter bf, std::string fname, std::string start, std::string end)
        : bloom(std::move(bf)), filename(std::move(fname)), startKey(std::move(start)), endKey(std::move(end)) {}

    Node(size_t bloomSize, double falsePositiveRate)
        : filename("Memory"), bloom(bloomSize, falsePositiveRate) {}
    
};
