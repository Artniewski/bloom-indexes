# Experiment 5 (exp5) - Partition Size Analysis

## Overview
Experiment 5 analyzes how bloom filter performance varies with different partition sizes (`itemsPerPartition`). It focuses on partition efficiency and real data percentage analysis to understand how bloom filter partitioning affects query performance and false positive rates.

## Test Configuration
- **Database Size**: Configurable (typically 50M records)
- **Columns Tested**: `phone`, `mail`, `address` (3 columns, fixed)
- **Items Per Partition Tested**: 50,000, 75,000, 100,000
- **Bloom Filter Settings**: 1M size, 6 hash functions
- **Real Data Percentages**: 0%, 20%, 33%, 50%, 75%, 100%
- **Query Scenarios**: 100 queries per real data percentage scenario

---

## CSV Files Generated

### 1. **Basic Performance Metrics**

#### `exp_5_basic_timings.csv` - Query Execution Times by Partition Size
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `itemsPerPartition` | Number of items per bloom filter partition (50K, 75K, 100K) |
| `falsePositiveProbability` | Theoretical false positive probability for the partition size |
| `globalScanTime` | Average time for full database scan (microseconds) |
| `hierarchicalSingleTime` | Average time for single-column hierarchical query (microseconds) |
| `hierarchicalMultiTime` | Average time for multi-column hierarchical query (microseconds) |

#### `exp_5_basic_checks.csv` - Basic Check Counts by Partition Size
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `itemsPerPartition` | Number of items per bloom filter partition |
| `multiBloomChecks` | Average bloom filter checks for multi-column queries |
| `multiLeafBloomChecks` | Average leaf bloom filter checks for multi-column queries |
| `multiSSTChecks` | Average SST file checks for multi-column queries |
| `singleBloomChecks` | Average bloom filter checks for single-column queries (raw count) |
| `singleLeafBloomChecks` | Average leaf bloom filter checks for single-column queries (raw count) |
| `singleSSTChecks` | Average SST file checks for single-column queries (raw count) |

**Note**: Single-column metrics are raw counts, not per-column normalized, since single-column queries only use one column.

---

### 2. **Per-Column Efficiency Analysis**

#### `exp_5_per_column_metrics.csv` - Multi-Column Efficiency per Partition Size
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `itemsPerPartition` | Number of items per bloom filter partition |
| `numColumns` | Number of columns tested (3 - for reference) |
| `multiBloomPerCol` | Average bloom checks per column for multi-column queries |
| `multiLeafPerCol` | Average leaf bloom checks per column for multi-column queries |
| `multiNonLeafPerCol` | Average non-leaf bloom checks per column for multi-column queries |
| `multiSSTPerCol` | Average SST checks per column for multi-column queries |

**Key Insight**: Shows how efficiently each partition size handles the work per column in multi-column scenarios.

---

### 3. **Real Data Percentage Analysis**

#### `exp_5_real_data_checks.csv` - Detailed Check Metrics by Real Data %
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `itemsPerPartition` | Number of items per bloom filter partition |
| `realDataPercentage` | Percentage of queries using real data (0%, 20%, 33%, 50%, 75%, 100%) |
| `avgMultiBloomChecks` | Average bloom filter checks for multi-column queries |
| `avgMultiLeafBloomChecks` | Average leaf bloom filter checks for multi-column queries |
| `avgMultiNonLeafBloomChecks` | Average non-leaf bloom filter checks for multi-column queries |
| `avgMultiSSTChecks` | Average SST file checks for multi-column queries |
| `avgSingleBloomChecks` | Average bloom filter checks for single-column queries (raw count) |
| `avgSingleLeafBloomChecks` | Average leaf bloom filter checks for single-column queries (raw count) |
| `avgSingleNonLeafBloomChecks` | Average non-leaf bloom filter checks for single-column queries (raw count) |
| `avgSingleSSTChecks` | Average SST file checks for single-column queries (raw count) |
| `avgRealMultiBloomChecks` | Average bloom filter checks for real data multi-column queries |
| `avgRealMultiSSTChecks` | Average SST file checks for real data multi-column queries |
| `avgFalseMultiBloomChecks` | Average bloom filter checks for false data multi-column queries |
| `avgFalseMultiSSTChecks` | Average SST file checks for false data multi-column queries |

#### `exp_5_real_data_per_column.csv` - Per-Column Metrics by Real Data %
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `itemsPerPartition` | Number of items per bloom filter partition |
| `realDataPercentage` | Percentage of queries using real data |
| `numColumns` | Number of columns tested (for reference) |
| `avgMultiBloomPerCol` | Average bloom checks per column for multi-column queries |
| `avgMultiLeafPerCol` | Average leaf bloom checks per column for multi-column queries |
| `avgMultiNonLeafPerCol` | Average non-leaf bloom checks per column for multi-column queries |
| `avgMultiSSTPerCol` | Average SST checks per column for multi-column queries |
| `avgRealMultiBloomPerCol` | Average bloom checks per column for real data multi-column queries |
| `avgRealMultiSSTPerCol` | Average SST checks per column for real data multi-column queries |
| `avgFalseMultiBloomPerCol` | Average bloom checks per column for false data multi-column queries |
| `avgFalseMultiSSTPerCol` | Average SST checks per column for false data multi-column queries |

**Note**: Only multi-column queries get per-column treatment since single-column queries don't scale with column count.

---

### 4. **Visualization-Focused Summaries**

#### `exp_5_partition_efficiency.csv` - Key Efficiency Metrics
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `itemsPerPartition` | Number of items per bloom filter partition |
| `realDataPercentage` | Percentage of queries using real data |
| `falsePositiveProbability` | Theoretical false positive probability |
| `avgMultiTime` | Average execution time for multi-column queries (microseconds) |
| `avgSingleTime` | Average execution time for single-column queries (microseconds) |
| `avgMultiBloomPerCol` | Average bloom checks per column for multi-column queries |
| `avgMultiSSTPerCol` | Average SST checks per column for multi-column queries |

**Visualization Use**: Perfect for creating charts showing how performance varies with partition size across different real data percentages.

#### `exp_5_timing_comparison.csv` - Real vs False Data Performance
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `itemsPerPartition` | Number of items per bloom filter partition |
| `realDataPercentage` | Percentage of queries using real data |
| `avgRealMultiTime` | Average execution time for real data multi-column queries (microseconds) |
| `avgRealSingleTime` | Average execution time for real data single-column queries (microseconds) |
| `avgFalseMultiTime` | Average execution time for false data multi-column queries (microseconds) |
| `avgFalseSingleTime` | Average execution time for false data single-column queries (microseconds) |

**Visualization Use**: Shows false positive performance impact across different partition sizes.

---

### 5. **Reference Data**

#### `exp_5_bloom_metrics.csv` - Bloom Filter Configuration
| Column | Description |
|--------|-------------|
| `dbSize` | Number of records in the database |
| `itemsPerPartition` | Number of items per bloom filter partition |
| `falsePositiveProbability` | Theoretical false positive probability |
| `leafs` | Number of leaf nodes in the bloom tree hierarchy |
| `diskBloomSize` | Total disk space used by bloom filters (bytes) |
| `memoryBloomSize` | Total memory used by bloom filters (bytes) |

---

## Key Analysis Dimensions

### Partition Size Effects
- **False Positive Rate**: How partition size affects theoretical and actual FPP
- **Memory Usage**: Trade-off between partition size and memory consumption
- **Query Performance**: Optimal partition size for different query patterns

### Real Data Percentage Impact
- **False Positive Cost**: How false queries affect performance with different partition sizes
- **Efficiency Trends**: Whether false positive impact varies with partition size
- **Optimization Points**: Finding optimal partition size for different real data ratios

### Visualization Recommendations

#### 1. **Partition Efficiency Charts**
- **X-axis**: Items per partition (50K, 75K, 100K)
- **Y-axis**: Per-column metrics or timing
- **Series**: Different real data percentages
- **Data Source**: `exp_5_partition_efficiency.csv`, `exp_5_per_column_metrics.csv`

#### 2. **False Positive Impact Charts**
- **X-axis**: Real data percentage (0% to 100%)
- **Y-axis**: Execution time or check counts
- **Series**: Different partition sizes
- **Data Source**: `exp_5_timing_comparison.csv`, `exp_5_real_data_checks.csv`

#### 3. **Partition Size Comparison**
- **X-axis**: Partition size
- **Y-axis**: Performance metrics
- **Series**: Multi-column vs single-column queries
- **Data Source**: `exp_5_basic_timings.csv`, `exp_5_basic_checks.csv`

#### 4. **Efficiency vs FPP Trade-off**
- **X-axis**: False positive probability
- **Y-axis**: Per-column efficiency
- **Bubbles**: Different partition sizes
- **Data Source**: `exp_5_partition_efficiency.csv`

---

## Key Research Questions Answered

1. **What is the optimal partition size for different workloads?**
   - Compare performance across partition sizes for different real data percentages
   - Use `exp_5_partition_efficiency.csv`

2. **How does partition size affect false positive impact?**
   - Analyze performance difference between real vs false data queries
   - Use `exp_5_timing_comparison.csv`

3. **What are the memory vs performance trade-offs?**
   - Compare bloom filter sizes against query performance
   - Use `exp_5_bloom_metrics.csv` with `exp_5_basic_timings.csv`

4. **How does per-column efficiency change with partition size?**
   - Analyze whether larger partitions provide better per-column efficiency
   - Use `exp_5_per_column_metrics.csv`

## Benefits for Visualization

1. **Partition-Focused**: All data organized around partition size as primary dimension
2. **FPP Integration**: False positive probability included for correlation analysis
3. **Raw vs Normalized**: Single-column data kept raw, multi-column data normalized per-column
4. **Efficiency Metrics**: Clear per-column efficiency data for multi-column scenarios
5. **Performance Separation**: Timing and check metrics in separate files for focused analysis

## Academic Value

This experiment provides insights into:
- **Bloom Filter Partitioning**: Optimal strategies for dividing data across bloom filter partitions
- **False Positive Management**: How partition size affects false positive rates and their performance impact
- **Multi-Column Efficiency**: Scaling patterns for multi-column bloom filter hierarchies
- **Memory-Performance Trade-offs**: Balancing memory usage against query performance 