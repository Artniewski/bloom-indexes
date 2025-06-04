# Experiment 1 (exp1) - Bloom Filter Performance Analysis

## Overview
Experiment 1 analyzes bloom filter performance across different database sizes and query patterns. It generates 10 focused CSV files, each containing specific metrics for different aspects of the analysis.

## Test Configuration
- **Database Sizes**: 10M, 20M, and configurable default records
- **Columns Tested**: `phone`, `mail`, `address` (3 columns)
- **Bloom Filter Settings**: 1M size, 6 hash functions, 100K items per partition
- **Query Scenarios**: Standard queries, pattern-based queries, and mixed real/false data queries

---

## CSV Files Generated

### 1. **Core Bloom Filter Metrics**

#### `exp_1_bloom_metrics.csv` - Bloom Filter Configuration & Size
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `bloomTreeRatio` | Ratio for bloom tree construction |
| `itemsPerPartition` | Number of items per bloom filter partition |
| `bloomSize` | Size of individual bloom filters in bits |
| `numHashFunctions` | Number of hash functions used |
| `singleHierarchyLeafs` | Number of leaf nodes in the bloom tree hierarchy |
| `bloomDiskSize` | Total disk space used by bloom filters (bytes) |
| `blomMemSize` | Total memory used by bloom filters (bytes) |

#### `exp_3_bloom_metrics.csv` - Creation Performance
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `bloomCreationTime` | Time to create bloom filter hierarchy (microseconds) |
| `dbCreationTime` | Time to create and initialize database (microseconds) |

---

### 2. **Basic Query Performance**

#### `exp_1_basic_metrics.csv` - Query Execution Times
| Column | Description |
|--------|-------------|
| `dbSize` | Number of records in the database |
| `globalScanTime` | Average time for full database scan (microseconds) |
| `hierarchicalSingleTime` | Average time for single-column hierarchical query (microseconds) |
| `hierarchicalMultiTime` | Average time for multi-column hierarchical query (microseconds) |

#### `exp_1_basic_checks.csv` - Basic Check Counts
| Column | Description |
|--------|-------------|
| `dbSize` | Number of records in the database |
| `multiBloomChecks` | Average bloom filter checks for multi-column queries |
| `multiLeafBloomChecks` | Average leaf bloom filter checks for multi-column queries |
| `multiSSTChecks` | Average SST file checks for multi-column queries |
| `singleBloomChecks` | Average bloom filter checks for single-column queries |
| `singleLeafBloomChecks` | Average leaf bloom filter checks for single-column queries |
| `singleSSTChecks` | Average SST file checks for single-column queries |

---

### 3. **Pattern-Based Analysis**

#### `exp_1_pattern_timings.csv` - Pattern Query Performance
| Column | Description |
|--------|-------------|
| `dbSize` | Number of records in the database |
| `percentageExisting` | Percentage of columns with existing values (0% = all false, 100% = all true) |
| `hierarchicalSingleTime` | Query time for single-column hierarchical approach (microseconds) |
| `hierarchicalMultiTime` | Query time for multi-column hierarchical approach (microseconds) |

**Pattern Explanation**: Tests queries where different percentages of columns contain real vs. fake data to measure false positive impact.

---

### 4. **Real Data Percentage Analysis** 

#### `exp_1_comprehensive_checks.csv` - Detailed Check Metrics by Real Data %
| Column | Description |
|--------|-------------|
| `dbSize` | Number of records in the database |
| `realDataPercentage` | Percentage of queries using real data (0%, 20%, 33%, 50%, 75%, 100%) |
| `avgMultiBloomChecks` | Average bloom filter checks for multi-column queries |
| `avgMultiLeafBloomChecks` | Average leaf bloom filter checks for multi-column queries |
| `avgMultiNonLeafBloomChecks` | Average non-leaf bloom filter checks (bloom - leaf) for multi-column |
| `avgMultiSSTChecks` | Average SST file checks for multi-column queries |
| `avgSingleBloomChecks` | Average bloom filter checks for single-column queries |
| `avgSingleLeafBloomChecks` | Average leaf bloom filter checks for single-column queries |
| `avgSingleNonLeafBloomChecks` | Average non-leaf bloom filter checks (bloom - leaf) for single-column |
| `avgSingleSSTChecks` | Average SST file checks for single-column queries |
| `avgRealMultiBloomChecks` | Average bloom filter checks for real data multi-column queries |
| `avgRealMultiSSTChecks` | Average SST file checks for real data multi-column queries |
| `avgFalseMultiBloomChecks` | Average bloom filter checks for false data multi-column queries |
| `avgFalseMultiSSTChecks` | Average SST file checks for false data multi-column queries |

#### `exp_1_per_column_stats.csv` - Per-Column Normalized Metrics
| Column | Description |
|--------|-------------|
| `dbSize` | Number of records in the database |
| `realDataPercentage` | Percentage of queries using real data |
| `numColumns` | Number of columns tested (for reference) |
| `avgMultiBloomPerCol` | Average bloom checks per column for multi-column queries |
| `avgMultiLeafPerCol` | Average leaf bloom checks per column for multi-column queries |
| `avgMultiNonLeafPerCol` | Average non-leaf bloom checks per column for multi-column queries |
| `avgMultiSSTPerCol` | Average SST checks per column for multi-column queries |
| `avgSingleBloomPerCol` | Average bloom checks per column for single-column queries |
| `avgSingleLeafPerCol` | Average leaf bloom checks per column for single-column queries |
| `avgSingleNonLeafPerCol` | Average non-leaf bloom checks per column for single-column queries |
| `avgSingleSSTPerCol` | Average SST checks per column for single-column queries |
| `avgRealMultiBloomPerCol` | Average bloom checks per column for real data multi-column queries |
| `avgRealMultiSSTPerCol` | Average SST checks per column for real data multi-column queries |
| `avgFalseMultiBloomPerCol` | Average bloom checks per column for false data multi-column queries |
| `avgFalseMultiSSTPerCol` | Average SST checks per column for false data multi-column queries |

**Per-Column Explanation**: All check counts are divided by the number of columns to enable comparison across different column counts.

---

### 5. **Mixed Query Analysis**

#### `exp_1_mixed_query_summary.csv` - Query Mix Summary
| Column | Description |
|--------|-------------|
| `dbSize` | Number of records in the database |
| `realDataPercentage` | Percentage of queries using real data |
| `totalQueries` | Total number of queries executed |
| `realQueries` | Number of queries with real data |
| `falseQueries` | Number of queries with false data |
| `avgMultiTime` | Average execution time for multi-column queries (microseconds) |
| `avgSingleTime` | Average execution time for single-column queries (microseconds) |
| `avgMultiBloomChecks` | Average bloom filter checks for multi-column queries |
| `avgMultiSSTChecks` | Average SST file checks for multi-column queries |

#### `exp_1_timing_comparison.csv` - Real vs False Data Performance
| Column | Description |
|--------|-------------|
| `dbSize` | Number of records in the database |
| `realDataPercentage` | Percentage of queries using real data |
| `avgRealMultiTime` | Average execution time for real data multi-column queries (microseconds) |
| `avgRealSingleTime` | Average execution time for real data single-column queries (microseconds) |
| `avgFalseMultiTime` | Average execution time for false data multi-column queries (microseconds) |
| `avgFalseSingleTime` | Average execution time for false data single-column queries (microseconds) |

---

### 6. **Legacy Compatibility Files**

#### `exp_4_query_timings.csv` - Basic Timing Compatibility
| Column | Description |
|--------|-------------|
| `dbSize` | Number of records in the database |
| `globalScanTime` | Average time for full database scan (microseconds) |
| `hierarchicalMultiColumnTime` | Average time for multi-column hierarchical query (microseconds) |
| `hierarchicalSingleColumnTime` | Average time for single-column hierarchical query (microseconds) |

---

## Key Metrics Explained

### Check Types
- **Bloom Checks**: Total bloom filter lookups performed
- **Leaf Bloom Checks**: Bloom filter lookups in leaf nodes (subset of total)
- **Non-Leaf Bloom Checks**: Bloom filter lookups in internal nodes (bloom - leaf)
- **SST Checks**: Direct SST file access operations

### Query Types
- **Single-Column**: Uses hierarchy from first column only
- **Multi-Column**: Uses hierarchies from all columns simultaneously
- **Real Data**: Queries for values that exist in the database
- **False Data**: Queries for values that don't exist in the database

### Real Data Percentage Scenarios
- **0%**: All queries use non-existent data (pure false positives test)
- **20%**: 1 in 5 queries uses real data
- **33%**: 1 in 3 queries uses real data  
- **50%**: Half queries use real data
- **75%**: 3 in 4 queries use real data
- **100%**: All queries use real data (no false positives)

## Usage
This data enables analysis of:
1. **Bloom filter effectiveness** across different data sizes
2. **False positive impact** on query performance
3. **Per-column efficiency** for multi-column scenarios
4. **Real vs false data performance** characteristics
5. **Scalability patterns** across database sizes 