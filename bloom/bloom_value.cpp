#include "bloom_value.hpp"

void bloom_value::insert(const std::string& key) {
  // Hash key using multiple hash functions and set corresponding bits
  // This is a simplified example, actual hashing functions would need to be
  // implemented
  for (int i = 0; i < numHashFunctions; ++i) {
    size_t hash = std::hash<std::string>{}(key + std::to_string(i));
    // print setting hahs bit
    // std::cout << "Setting bit: " << hash % bitArray.size() << std::endl;
    bitArray.set(hash % bitArray.size(), true);
  }
}

void bloom_value::saveToFile(const std::string& filename) {
  std::ofstream file(filename, std::ios::binary);
  if (!file) {
    std::cerr << "Error opening file: " << filename << std::endl;
    return;
  }

  // Write the number of hash functions used
  file.write(reinterpret_cast<const char*>(&numHashFunctions),
             sizeof(numHashFunctions));

  // Write the bit array
  std::vector<char> buffer((bitArray.size() + 7) /
                           8);  // Convert bitset to byte array
  for (size_t i = 0; i < bitArray.size(); ++i) {
    buffer[i / 8] |= bitArray[i] << (i % 8);
  }
  file.write(buffer.data(), buffer.size());

  file.close();
}

bloom_value bloom_value::loadFromFile(const std::string& filename) {
  
  std::ifstream file(filename, std::ios::binary);

  if (!file) {
    throw std::runtime_error("Error opening file: " + filename);
  }

  // Read the number of hash functions used
  int numHashFunctions;
  file.read(reinterpret_cast<char*>(&numHashFunctions),
            sizeof(numHashFunctions));

   // Read the bit array
  std::vector<char> buffer((bitArray.size() + 7) /
                           8);  // Assuming the same size as saved in saveToFile
  file.read(buffer.data(), buffer.size());

  bloom_value filter;

  // Convert byte array to bitset
  for (size_t i = 0; i < filter.bitArray.size(); ++i) {
    filter.bitArray[i] = (buffer[i / 8] >> (i % 8)) & 1;
  }

  file.close();

  return filter;
}


void bloom_value::createFile(const std::string& filename) {
  std::ofstream file(filename, std::ios::binary);
  if (!file) {
    std::cerr << "Error opening file: " << filename << std::endl;
    return;
  }
}


// Checks if at least one element in key[] exists in Bloom filter 
bool bloom_value::exists(const std::string key[]) const {

  for (size_t i=0; i<key->length(); i++) {
    if (exists(key[i]))
      return true;
  }
  return false;

}

bool bloom_value::exists(const std::string& key) const {
  // Check if all bits corresponding to the key are set
  for (int i = 0; i < numHashFunctions; ++i) {
    size_t hash = std::hash<std::string>{}(key + std::to_string(i));
    if (!bitArray[hash % bitArray.size()]) {
      return false;  // If any bit is not set, the element definitely doesn't
                     // exist
    }
  }
  return true;  // If all bits are set, the element might exist
};
