#pragma once

#include <cmath>
#include <fstream>
#include <string>
#include <vector>

class BloomFilter {
   private:
    size_t hash(const std::string& key, int seed) const;

   public:
    std::vector<bool> bitArray;
    int numHashFunctions;
    size_t bitArraySize;
    //  for future use
    // BloomFilter(size_t expectedItems, double falsePositiveRate);
    BloomFilter(size_t size, double numHashFunctions);
    void insert(const std::string& key);
    bool exists(const std::string& key) const;
    void merge(const BloomFilter& other);

    void saveToFile(const std::string& filename) const;
    static BloomFilter loadFromFile(const std::string& filename);
};
