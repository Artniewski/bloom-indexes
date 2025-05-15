#pragma once

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <future>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>

#include "bloomTree.hpp"
#include "db_manager.hpp"
#include "node.hpp"
#include "stopwatch.hpp"

extern boost::asio::thread_pool globalThreadPool;

/// Global counter of bloom‐filter lookups performed
inline std::atomic<size_t> gBloomCheckCount{0};

// Combination of nodes
struct Combo {
    std::vector<Node*> nodes;  // One node per column.
    std::string rangeStart;
    std::string rangeEnd;
};

inline std::vector<std::string> globalfinalMatches;

void computeIntersection(const std::vector<Node*>& nodes, std::string& outStart, std::string& outEnd) {
    if (nodes.empty()) return;
    outStart = nodes[0]->startKey;
    outEnd = nodes[0]->endKey;
    for (size_t i = 1; i < nodes.size(); ++i) {
        outStart = std::max(outStart, nodes[i]->startKey);
        outEnd = std::min(outEnd, nodes[i]->endKey);
    }
}

std::vector<std::string> finalSstScanAndIntersect(const Combo& combo,
                                                  const std::vector<std::string>& values,
                                                  DBManager& dbManager) {
    size_t n = combo.nodes.size();

    std::vector<std::promise<std::unordered_set<std::string>>> promises(n);
    std::vector<std::future<std::unordered_set<std::string>>> futures;
    futures.reserve(n);
    
    for (size_t i = 0; i < n; ++i) {
        futures.push_back(promises[i].get_future());
        Node* leaf = combo.nodes[i];
        std::string scanStart = std::max(combo.rangeStart, leaf->startKey);
        std::string scanEnd = std::min(combo.rangeEnd, leaf->endKey);
        
        boost::asio::post(globalThreadPool,
            [leaf, values, i, scanStart, scanEnd, &dbManager, promise = std::move(promises[i])]() mutable {
                try {
                    // Scan the SST file for keys matching the value.
                    std::vector<std::string> keys = dbManager.scanFileForKeysWithValue(leaf->filename, values[i], scanStart, scanEnd);
                    promise.set_value(std::unordered_set<std::string>(keys.begin(), keys.end()));
                } catch (const std::exception& e) {
                    promise.set_exception(std::current_exception());
                }
            });
    }

    // Collect the key sets from all futures.
    std::vector<std::unordered_set<std::string>> columnKeySets;
    columnKeySets.reserve(n);
    for (auto& fut : futures) {
        columnKeySets.push_back(fut.get());
    }

    // Intersect all key sets
    if (columnKeySets.empty()) return {};

    std::unordered_set<std::string> result = columnKeySets[0];
    for (size_t i = 1; i < columnKeySets.size(); ++i) {
        std::unordered_set<std::string> temp;
        for (const auto& key : result) {
            if (columnKeySets[i].find(key) != columnKeySets[i].end()) {
                temp.insert(key);
            }
        }
        result = std::move(temp);
        if (result.empty())
            break;
    }

    return std::vector<std::string>(result.begin(), result.end());
}

// DFS with combinations level by level,
// using only children that pass the Bloom filter.
void dfsMultiColumn(const std::vector<std::string>& values, Combo currentCombo, DBManager& dbManager) {
    // check if all nodes pass Bloom
    for (size_t i = 0; i < currentCombo.nodes.size(); ++i) {
        ++gBloomCheckCount;
        if (!currentCombo.nodes[i]->bloom.exists(values[i])) {
            return;  // Prune this branch.
        }
    }
    // Check if range is valid
    if (currentCombo.rangeStart > currentCombo.rangeEnd) {
        return;
    }

    // If all nodes are leaves, do final SST scans
    bool allLeaves = true;
    for (auto* node : currentCombo.nodes) {
        if (node->filename == "Memory") {
            allLeaves = false;
            break;
        }
    }
    if (allLeaves) {
        std::vector<std::string> keys = finalSstScanAndIntersect(currentCombo, values, dbManager);
        globalfinalMatches.insert(globalfinalMatches.end(), keys.begin(), keys.end());
        return;
    }

    // For each column, gather candidate children that pass the Bloom filter.
    size_t n = currentCombo.nodes.size();
    std::vector<std::vector<Node*>> candidateOptions(n);
    for (size_t i = 0; i < n; ++i) {
        Node* currentNode = currentCombo.nodes[i];
        if (currentNode->filename != "Memory") {
            candidateOptions[i] = {currentNode};
        } else {
            for (Node* child : currentNode->children) {
                // Only include the child if it passes the Bloom filter.
                ++gBloomCheckCount;
                if (child->bloom.exists(values[i])) {
                    candidateOptions[i].push_back(child);
                }
            }
            // If no child passes, prune this branch.
            if (candidateOptions[i].empty()) {
                return;
            }
        }
    }

    // Cartesian product of candidateOptions for all columns
    // recursive backtrack lambda
    std::function<void(size_t, std::vector<Node*>&)> backtrack =
        [&](size_t idx, std::vector<Node*>& chosen) {
            if (idx == n) {
                // intersection of nodes' key ranges
                std::string newStart, newEnd;
                computeIntersection(chosen, newStart, newEnd);
                if (newStart <= newEnd) {
                    Combo nextCombo{chosen, newStart, newEnd};
                    dfsMultiColumn(values, nextCombo, dbManager);
                }
                return;
            }
            for (Node* candidate : candidateOptions[idx]) {
                chosen[idx] = candidate;
                backtrack(idx + 1, chosen);
            }
        };

    std::vector<Node*> chosen(n, nullptr);
    backtrack(0, chosen);
}

// Multi-column hierarchical query interface.
std::vector<std::string> multiColumnQueryHierarchical(std::vector<BloomTree>& trees,
                                                      const std::vector<std::string>& values,
                                                      const std::string& globalStart,
                                                      const std::string& globalEnd,
                                                      DBManager& dbManager) {
    StopWatch sw;
    sw.start();
    if (trees.size() != values.size() || trees.empty()) {
        std::cerr << "Error: Number of trees and values must match and be non-empty.\n";
        sw.stop();
        return {};
    }
    size_t n = trees.size();
    Combo startCombo;
    startCombo.nodes.resize(n);
    std::string s = globalStart.empty() ? trees[0].root->startKey : globalStart;
    std::string e = globalEnd.empty() ? trees[0].root->endKey : globalEnd;
    for (size_t i = 0; i < n; ++i) {
        Node* root = trees[i].root;
        startCombo.nodes[i] = root;
        s = std::max(s, root->startKey);
        e = std::min(e, root->endKey);
    }
    startCombo.rangeStart = s;
    startCombo.rangeEnd = e;

    globalfinalMatches.clear();
    dfsMultiColumn(values, startCombo, dbManager);
    sw.stop();
    spdlog::critical("Multi-column query with SST scan took {} µs, found matching {} keys.", sw.elapsedMicros(), globalfinalMatches.size());
    return globalfinalMatches;
}
