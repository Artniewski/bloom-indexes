#pragma once

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <functional>
#include <future>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include "bloomTree.hpp"
#include "db_manager.hpp"
#include "node.hpp"
#include "stopwatch.hpp"

extern boost::asio::thread_pool globalThreadPool;

/// Global counter of bloom‐filter lookups performed
inline std::atomic<size_t> gBloomCheckCount{0};
/// Global counter of leaf-node bloom-filter lookups performed
inline std::atomic<size_t> gLeafBloomCheckCount{0};
/// Global counter of SSTables checked
inline std::atomic<size_t> gSSTCheckCount{0};

// Combination of nodes
struct Combo {
  std::vector<Node*> nodes;  // One node per column.
  std::string rangeStart;
  std::string rangeEnd;
};

inline std::vector<std::string> globalfinalMatches;

inline void computeIntersection(const std::vector<Node*>& nodes,
                                std::string& outStart, std::string& outEnd) {
  if (nodes.empty()) return;
  outStart = nodes[0]->startKey;
  outEnd = nodes[0]->endKey;
  for (size_t i = 1; i < nodes.size(); ++i) {
    outStart = std::max(outStart, nodes[i]->startKey);
    outEnd = std::min(outEnd, nodes[i]->endKey);
  }
}

inline std::vector<std::string> finalSstScanAndIntersect(
    const Combo& combo, const std::vector<std::string>& values,
    DBManager& dbManager) {
  size_t n = combo.nodes.size();

  // Increment SSTable check count
  gSSTCheckCount += n;

  std::vector<std::promise<std::unordered_set<std::string>>> promises(n);
  std::vector<std::future<std::unordered_set<std::string>>> futures;
  futures.reserve(n);

  for (size_t i = 0; i < n; ++i) {
    futures.push_back(promises[i].get_future());
    Node* leaf = combo.nodes[i];
    std::string scanStart = std::max(combo.rangeStart, leaf->startKey);
    std::string scanEnd = std::min(combo.rangeEnd, leaf->endKey);

    boost::asio::post(
        globalThreadPool, [leaf, values, i, scanStart, scanEnd, &dbManager,
                           promise = std::move(promises[i])]() mutable {
          try {
            // Scan the SST file for keys matching the value.
            std::vector<std::string> keys = dbManager.scanFileForKeysWithValue(
                leaf->filename, values[i], scanStart, scanEnd);
            promise.set_value(
                std::unordered_set<std::string>(keys.begin(), keys.end()));
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
    if (result.empty()) break;
  }

  return std::vector<std::string>(result.begin(), result.end());
}

// DFS with per‑level range pruning and optional first‑column parallel split
inline void dfsMultiColumn(const std::vector<std::string>& values,
                           Combo currentCombo, DBManager& dbManager,
                           bool isInitialCall) {
                            //check roots
if (isInitialCall) {
  for (size_t i = 0; i < currentCombo.nodes.size(); ++i) {
    ++gBloomCheckCount;
    if (!currentCombo.nodes[i]->bloom.exists(values[i]))
      return;
  }
}

  // 2) range check
  if (currentCombo.rangeStart > currentCombo.rangeEnd) return;

  // 3) leaf‑check
  bool allLeaves = true;
  for (auto* nd : currentCombo.nodes) {
    if (nd->filename == "Memory") {
      allLeaves = false;
      break;
    }
  }
  if (allLeaves) {
    auto keys = finalSstScanAndIntersect(currentCombo, values, dbManager);
    globalfinalMatches.insert(globalfinalMatches.end(), keys.begin(),
                              keys.end());
    return;
  }

  // 4) build candidateOptions with progressive range tightening
  size_t n = currentCombo.nodes.size();
  std::vector<std::vector<Node*>> candidateOptions(n);
  std::string tightStart = currentCombo.rangeStart;
  std::string tightEnd = currentCombo.rangeEnd;

  for (size_t i = 0; i < n; ++i) {
    Node* node = currentCombo.nodes[i];
    std::string colMin, colMax;
    bool found = false;

    auto consider = [&](Node* c) {
      if (c->endKey < tightStart || c->startKey > tightEnd) return;
      ++gBloomCheckCount;
      if (c->filename != "Memory") ++gLeafBloomCheckCount;
      if (!c->bloom.exists(values[i])) return;
      candidateOptions[i].push_back(c);
      if (!found) {
        colMin = c->startKey;
        colMax = c->endKey;
        found = true;
      } else {
        colMin = std::min(colMin, c->startKey);
        colMax = std::max(colMax, c->endKey);
      }
    };

    if (node->filename == "Memory") {
      for (auto* ch : node->children) consider(ch);
    } else {
      consider(node);
    }
    if (!found) return;

    if (i + 1 < n) {
      tightStart = std::max(tightStart, colMin);
      tightEnd = std::min(tightEnd, colMax);
      if (tightStart > tightEnd) return;
    }
  }

  // 5) prepare backtrack that carries (curStart,curEnd)
  std::function<void(size_t, std::vector<Node*>&, const std::string&,
                     const std::string&)>
      backtrack;

  backtrack = [&](size_t idx, std::vector<Node*>& chosen,
                  const std::string& curS, const std::string& curE) {
    if (idx == n) {
      Combo next{chosen, curS, curE};
      dfsMultiColumn(values, next, dbManager, false);
      return;
    }
    for (auto* cand : candidateOptions[idx]) {
      auto ns = std::max(curS, cand->startKey);
      auto ne = std::min(curE, cand->endKey);
      if (ns <= ne) {
        chosen[idx] = cand;
        backtrack(idx + 1, chosen, ns, ne);
      }
    }
  };

  std::vector<Node*> chosen(n, nullptr);
  backtrack(0, chosen, currentCombo.rangeStart, currentCombo.rangeEnd);
}

// Multi-column hierarchical query interface.
inline std::vector<std::string> multiColumnQueryHierarchical(
    std::vector<BloomTree>& trees, const std::vector<std::string>& values,
    const std::string& globalStart, const std::string& globalEnd,
    DBManager& dbManager) {
  StopWatch sw;
  sw.start();
  size_t n = trees.size();
  if (n == 0 || n != values.size()) {
    std::cerr
        << "Error: Number of trees and values must match and be non-empty.\n";
    sw.stop();
    return {};
  }

  // Reset counters
  gBloomCheckCount = 0;
  gLeafBloomCheckCount = 0;
  gSSTCheckCount = 0;

  Combo start;
  start.nodes.resize(n);
  std::string s = globalStart.empty() ? trees[0].root->startKey : globalStart;
  std::string e = globalEnd.empty() ? trees[0].root->endKey : globalEnd;
  for (size_t i = 0; i < n; ++i) {
    start.nodes[i] = trees[i].root;
    s = std::max(s, trees[i].root->startKey);
    e = std::min(e, trees[i].root->endKey);
  }
  start.rangeStart = s;
  start.rangeEnd = e;
  globalfinalMatches.clear();
  dfsMultiColumn(values, start, dbManager, true);

  sw.stop();
  spdlog::critical(
      "Multi-column query with SST scan took {} µs, found matching {} keys.",
      sw.elapsedMicros(), globalfinalMatches.size());
  spdlog::info(
      "Bloom filters checked: {} (total), {} (leaves only), SSTables checked: "
      "{}",
      gBloomCheckCount.load(), gLeafBloomCheckCount.load(),
      gSSTCheckCount.load());
  return globalfinalMatches;
}
