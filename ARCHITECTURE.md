# System Architecture

This document provides a high-level overview of the HierarchicalDB system architecture, detailing the interaction between its core components and the data flow during query processing and index construction.

## Core Components

The system is composed of the following major components:

1.  **RocksDB**:
    *   The underlying persistent storage engine.
    *   Stores data in key-value pairs, organized into Column Families. Each logical column of the dataset (e.g., "phone", "mail") is typically mapped to a separate Column Family.
    *   Data is stored in SST (Sorted String Table) files on disk.

2.  **DBManager (`db_manager.cpp`, `db_manager.hpp`)**:
    *   Acts as an abstraction layer over RocksDB.
    *   Handles database creation, opening, and closing.
    *   Manages Column Family creation and handles.
    *   Provides methods for inserting data into RocksDB (e.g., `insertRecords`, `insertRecordsWithSearchTargets`). This includes batching writes and flushing data to SST files.
    *   Provides methods for triggering manual compaction of Column Families (`compactAllColumnFamilies`).
    *   Provides methods to scan SST files for a given Column Family (`scanSSTFilesForColumn`), which is crucial for building the Bloom filter hierarchies.
    *   Provides methods to retrieve specific values or scan for keys matching certain criteria directly from RocksDB or specific SST files (e.g., `getValue`, `scanFileForKeysWithValue`).

3.  **BloomManager (`bloom_manager.cpp`, `bloom_manager.hpp`)**:
    *   Responsible for creating and managing Bloom filter hierarchies (`BloomTree`).
    *   Takes a list of SST files (obtained via `DBManager`) for a column as input.
    *   Reads key-value pairs from these SST files.
    *   Constructs a `BloomTree` for each indexed column. This involves:
        *   Partitioning the key space of the SST files.
        *   Creating `Node` objects for these partitions.
        *   Generating a Bloom filter for the values within each node's key range.
        *   Organizing these nodes into a tree structure based on key ranges.

4.  **BloomTree (`bloomTree.cpp`, `bloomTree.hpp`)**:
    *   The primary hierarchical index structure. A separate `BloomTree` is maintained for each indexed column.
    *   Composed of `Node` objects.
    *   **`Node` (`node.cpp`, `node.hpp`)**:
        *   `keyRange`: The range of keys [startKey, endKey] covered by this node.
        *   `bloomFilter`: A Bloom filter containing all unique values present for keys within this node's `keyRange` in the specific column.
        *   `fileRef`: An identifier for the SST file (from RocksDB) where the actual data for this node's range primarily resides. For leaf nodes, this points to a specific SST. For internal nodes, this might be a more general indicator or "Memory" if it represents aggregated data not yet tied to a single SST.
        *   `children`: A list of child nodes, forming the hierarchical structure. Leaf nodes have no children.
    *   Provides methods to query the tree (e.g., `queryNodes`) to find nodes whose Bloom filters might contain a given value and whose key ranges overlap with a query range.

5.  **Query Engine (Primarily in `algorithm.hpp` and `main.cpp` for orchestrating experiments)**:
    *   Implements the multi-column query logic.
    *   **`multiColumnQuery`**:
        *   Takes a list of `BloomTree`s (one for each column in the query) and corresponding values to search for.
        *   Initiates a `dfsQuery`.
    *   **`dfsQuery`**:
        *   Performs a recursive depth-first search across the `BloomTree`s.
        *   Uses `Combo` objects to represent the current combination of nodes (one from each tree) being considered.
        *   **Pruning**:
            *   Checks the Bloom filter of each node in the `currentCombo` against the query value for that column. If a value is not found in a Bloom filter, that search path is pruned.
            *   Calculates the intersection of key ranges of all nodes in the `currentCombo`. If the intersection is empty, the path is pruned.
        *   **Leaf Node Processing**: If all nodes in a `Combo` are leaf nodes, it proceeds to `finalSstScan`.
        *   **Recursive Step**: For non-leaf nodes, it finds children whose Bloom filters pass the test and recursively calls `dfsQuery` on new `Combo`s formed from these children.
    *   **`finalSstScan`**:
        *   When `dfsQuery` reaches a `Combo` of all leaf nodes whose Bloom filters and key ranges are promising:
            *   For each leaf node in the `Combo`, it instructs `DBManager` to scan the corresponding SST file (identified by `fileRef` and constrained by the node's effective key range) to retrieve the actual keys that have the query value.
            *   The sets of keys retrieved from each SST file scan are then intersected to get the final set of keys satisfying the multi-column query.

## Data Flow

### 1. Index Building (Bloom Filter Hierarchy Creation)

1.  **Data Insertion**: Data is inserted into RocksDB via `DBManager`. Keys are typically prefixed strings (e.g., "key000...1"), and values are constructed based on column name and an index (e.g., "phone_value1"). Data is flushed to SST files.
2.  **SST File Discovery**: For each column to be indexed, `DBManager::scanSSTFilesForColumn` is called to get a list of all SST files belonging to that column family.
3.  **Hierarchy Construction (`BloomManager`)**:
    *   For each column:
        *   The `BloomManager` iterates through the identified SST files.
        *   It reads the key-value pairs from these files.
        *   It partitions the data (typically based on key ranges or a fixed number of items per partition) to create leaf `Node`s of the `BloomTree`.
        *   For each leaf `Node`, a Bloom filter is populated with all unique values found within its key range in that SST file(s). The `fileRef` is set to the SST file.
        *   Internal `Node`s are created by hierarchically grouping child nodes. The Bloom filter of an internal node is typically a union of its children's Bloom filters (or rebuilt from the underlying data spanning its children's key ranges). Their `keyRange` covers the union of their children's key ranges.

### 2. Multi-Column Querying

1.  **Query Initiation (`multiColumnQuery`)**: The user or an experiment script initiates a query, providing:
    *   A list of `BloomTree`s (one for each column involved in the query).
    *   A list of values to search for in the corresponding columns.
    *   An initial global key range (optional, can be derived from tree roots).
2.  **Depth-First Search (`dfsQuery`)**:
    *   Starts with a `Combo` of the root nodes of all participating `BloomTree`s.
    *   **Iteration 1 (Roots)**:
        *   For each root node in the `Combo`, its Bloom filter is checked against the query value for its column. If any check fails, the query might terminate early if no results are possible (though `dfsQuery` prunes paths, not necessarily the entire query at the root level unless all root paths are pruned).
        *   The intersection of the root nodes' key ranges is calculated.
    *   **Recursive Descent**:
        *   If Bloom filters pass and the key range intersection is valid:
            *   If all nodes in the `currentCombo` are leaf nodes:
                *   Proceed to `finalSstScan`.
            *   Else (at least one node is internal):
                *   For each internal node in the `currentCombo`, identify its child nodes.
                *   Filter these children: only keep those whose Bloom filter *might* contain the respective query value.
                *   Form new `Combo`s from the Cartesian product of these lists of candidate children.
                *   For each new `Combo`, calculate the intersection of its nodes' key ranges.
                *   If the new key range is valid, recursively call `dfsQuery` with the new `Combo`.
3.  **Final SST Scan and Intersection (`finalSstScan`)**:
    *   This function is invoked when `dfsQuery` reaches a `Combo` where all nodes are leaf nodes, and all preliminary Bloom filter and key range checks have passed.
    *   For each leaf `Node` in the `Combo`:
        *   The `DBManager` is used to scan the specific SST file (referenced by `node.fileRef`) for keys that fall within the node's effective key range (intersection of original node range and the `Combo`'s overall intersected range) AND have the target value for that column.
        *   This results in a set of candidate keys for each column.
    *   The sets of keys from each column are intersected. The final result of this intersection is the set of keys that satisfy all conditions of the multi-column query.
4.  **Result Aggregation**: The keys found by `finalSstScan` are added to a global list of results.

This architecture allows HierarchicalDB to efficiently prune large portions of the search space using the Bloom filter hierarchies, significantly reducing the amount of direct data access (SST file scans) required, especially for selective queries. 