#pragma once

#include <bitset>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>


class bloom_value {
 private:
  int numHashFunctions=3;

 public:
  std::bitset<2000000> bitArray;  
  bloom_value() {}
  void insert(const std::string& key);
  void saveToFile(const std::string& filename);
  void createFile(const std::string& filename);
  bloom_value loadFromFile(const std::string& filename);
  bool exists(const std::string& key) const;
  bool exists(const std::string key[]) const;
};
