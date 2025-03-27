#include "bloomTree.hpp"

#include <iostream>
#include <algorithm>

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
    // sort
    std::sort(nodes.begin(), nodes.end(), [](const Node* a, const Node* b) {
        return a->startKey < b->startKey;
    });

    for (size_t i = 0; i < nodes.size(); i += ratio) {
        size_t end = std::min(i + ratio, nodes.size());

        // set keys based on sorted nodes
        Node* parent = new Node(BloomFilter(bloomSize, numHashFunctions), "Memory",
                                nodes[i]->startKey, nodes[end - 1]->endKey);

        for (size_t j = i; j < end; ++j) {
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
