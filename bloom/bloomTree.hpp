#pragma once
#include <memory>
#include <vector>

#include "node.hpp"

class BloomTree {
   public:
    Node* root;

   private:
    int ratio;
    size_t bloomSize;
    int numHashFunctions;

    // for future use
    //  size_t expectedItems;
    //  double bloomFalsePositiveRate;

    void buildLevel(std::vector<Node*>& nodes);
    void search(Node* node, const std::string& value,
                const std::string& qStart, const std::string& qEnd,
                std::vector<std::string>& results) const;

    void searchNodes(Node* node, const std::string& value,
                     const std::string& qStart, const std::string& qEnd,
                     std::vector<const Node*>& results) const;

   public:
    // for future use
    //   BloomTree(int branchingRatio, size_t expectedItems, double bloomFalsePositiveRate)
    //       : ratio(branchingRatio),
    //       expectedItems(expectedItems),
    //         bloomFalsePositiveRate(bloomFalsePositiveRate) {}
    BloomTree(int branchingRatio, size_t bloomSize, int numHashFunctions)
        : ratio(branchingRatio),
          bloomSize(bloomSize),
          numHashFunctions(numHashFunctions) {}

    std::vector<Node*> leafNodes;

    void addLeafNode(BloomFilter&& bv, const std::string& file,
                     const std::string& start, const std::string& end);

    void buildTree();

    std::vector<std::string> query(const std::string& value,
                                   const std::string& qStart,
                                   const std::string& qEnd) const;

    std::vector<const Node*> queryNodes(const std::string& value,
                                        const std::string& qStart,
                                        const std::string& qEnd) const;
};
