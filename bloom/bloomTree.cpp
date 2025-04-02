#include "bloomTree.hpp"

#include <unistd.h>

#include <algorithm>
#include <climits>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <stdexcept>

void BloomTree::addLeafNode(BloomFilter&& bv, const std::string& file,
                            const std::string& start, const std::string& end) {
    leafNodes.push_back(new Node(std::move(bv), file, start, end));
}

void BloomTree::buildLevel(std::vector<Node*>& nodes) {
    if (nodes.size() == 1) {
        root = std::move(nodes.front());
        return;
    }

    std::vector<Node*> parentLevel;

    for (size_t i = 0; i < nodes.size(); i += ratio) {
        size_t end = std::min(i + ratio, nodes.size());

        Node* parent = new Node(BloomFilter(bloomSize, numHashFunctions), "Memory",
                                nodes[i]->startKey, nodes[end - 1]->endKey);

        for (size_t j = i; j < end; ++j) {
            if (parent->startKey > nodes[j]->startKey) {
                parent->startKey = nodes[j]->startKey;
            }
            if (parent->endKey < nodes[j]->endKey) {
                parent->endKey = nodes[j]->endKey;
            }
            parent->bloom.merge(nodes[j]->bloom);
            parent->children.push_back(std::move(nodes[j]));
        }

        parentLevel.push_back(parent);
    }

    buildLevel(parentLevel);
}

void BloomTree::buildTree() {
    buildLevel(leafNodes);
}

void BloomTree::search(Node* node, const std::string& value,
                       const std::string& qStart, const std::string& qEnd,
                       std::vector<std::string>& results) const {
    if (!node) return;

    bool overlaps =
        (qEnd.empty() || node->startKey <= qEnd) &&
        (qStart.empty() || node->endKey >= qStart);

    if (overlaps && node->bloom.exists(value)) {
        if (node->filename != "Memory") {
            results.push_back(node->filename);
        } else {
            for (Node* child : node->children) {
                search(child, value, qStart, qEnd, results);
            }
        }
    }
}

std::vector<std::string> BloomTree::query(const std::string& value,
                                          const std::string& qStart,
                                          const std::string& qEnd) const {
    std::vector<std::string> results;
    search(root, value, qStart, qEnd, results);
    return results;
}

// search that returns nodes
void BloomTree::searchNodes(Node* node, const std::string& value,
                            const std::string& qStart, const std::string& qEnd,
                            std::vector<const Node*>& results) const {
    if (!node) return;

    bool overlaps =
        (qEnd.empty() || node->startKey <= qEnd) &&
        (qStart.empty() || node->endKey >= qStart);

    if (overlaps && node->bloom.exists(value)) {
        if (node->children.empty()) {
            results.push_back(node);
        } else {
            for (Node* child : node->children) {
                searchNodes(child, value, qStart, qEnd, results);
            }
        }
    }
}

// query where return type is vector of nodes
std::vector<const Node*> BloomTree::queryNodes(const std::string& value,
                                               const std::string& qStart,
                                               const std::string& qEnd) const {
    std::vector<const Node*> results;
    searchNodes(root, value, qStart, qEnd, results);
    return results;
}

static size_t computeNodeMemory(const Node* node) {
    if (!node) return 0;
    size_t mem = 0;

    mem += sizeof(Node);
    mem += node->children.capacity() * sizeof(Node*);
    mem += node->filename.capacity() * sizeof(char);
    mem += node->startKey.capacity() * sizeof(char);
    mem += node->endKey.capacity() * sizeof(char);
    mem += node->bloom.bitArray.capacity() / CHAR_BIT;
    mem += sizeof(node->bloom.bitArray);

    for (const Node* child : node->children) {
        mem += computeNodeMemory(child);
    }
    return mem;
}

size_t BloomTree::memorySize() const {
    return computeNodeMemory(root);
}

static size_t computeBloomFilterDiskSize(const BloomFilter& bf) {
    char tmpName[] = "/tmp/bloomXXXXXX";
    int fd = mkstemp(tmpName);
    if (fd == -1) {
        throw std::runtime_error("mkstemp failed");
    }
    close(fd);

    bf.saveToFile(std::string(tmpName));

    size_t size = std::filesystem::file_size(tmpName);

    std::remove(tmpName);

    return size;
}

size_t BloomTree::diskSize() const {
    size_t total = 0;
    for (const Node* leaf : leafNodes) {
        if (leaf->filename != "Memory") {
            total += computeBloomFilterDiskSize(leaf->bloom);
        }
    }
    return total;
}