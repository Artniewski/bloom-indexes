#include <cmath>    // For std::exp and std::pow
#include <iomanip>  // For std::setw, std::fixed, std::setprecision
#include <iostream>
#include <string>
#include <vector>

// Function to calculate the False Positive Probability (FPP) of a Bloom filter
double calculate_bloom_fpp(long long n_items, long long m_bits, int k_hashes) {
  // Using the formula: p = (1 - e^(-k*n/m))^k
  // Ensure floating point division
  double exponent =
      -(static_cast<double>(k_hashes) * static_cast<double>(n_items)) /
      static_cast<double>(m_bits);
  double val_exp = std::exp(exponent);
  double base = 1.0 - val_exp;
  double fpp = std::pow(base, static_cast<double>(k_hashes));

  return fpp;
}

void run_parameter_sweep() {
  // Print CSV Header
  std::cout << "Items_n,Bits_Per_Item,Total_Bits_m,Hashes_k,FPP" << std::endl;

  // Define ranges for parameters
  std::vector<long long> items_per_partition_range = {
      20000, 50000, 150000, 100000, 200000, 500000, 1000000};
  std::vector<int> bits_per_item_range = {
      1, 2,  3,  4,  5,  6,  7,  8,
      9, 10, 11, 12, 13, 14, 15, 16};  // This will be used to calculate m_bits
  std::vector<int> num_hash_functions_range = {1, 2,  3,  4,  5,  6,  7,  8,
                                               9, 10, 11, 12, 13, 14, 15, 16};

  for (long long n_items : items_per_partition_range) {
    for (int bits_per_item : bits_per_item_range) {
      long long m_bits = n_items * bits_per_item;

      for (int k_hashes : num_hash_functions_range) {
        double fpp = 0.0;
        fpp = calculate_bloom_fpp(n_items, m_bits, k_hashes);
        // Output CSV row
        std::cout << n_items << "," << bits_per_item << "," << m_bits << ","
                  << k_hashes << "," << std::fixed << std::setprecision(8)
                  << fpp << std::endl;
      }
    }
  }
}

int main(int argc, char *argv[]) {
  run_parameter_sweep();
  return 0;
}