# Experiment 6 (exp6) - Bloom Filter Size Analysis

## Overview
Experiment 6 analyzes how bloom filter performance varies with different bloom filter sizes. It focuses on size efficiency and real data percentage analysis to understand how bloom filter memory allocation affects query performance and false positive rates.

## Test Configuration
- **Database Size**: Configurable (typically 50M records)
- **Columns Tested**: `phone`, `mail`, `address` (3 columns, fixed)
- **Bloom Filter Sizes Tested**: 500,000, 1,000,000, 2,000,000 bits
- **Partition Settings**: 100K items per partition, 6 hash functions (fixed)
- **Real Data Percentages**: 0%, 20%, 33%, 50%, 75%, 100%
- **Query Scenarios**: 100 queries per real data percentage scenario

---

## CSV Files Generated

### 1. **Basic Performance Metrics**

#### `exp_6_basic_timings.csv` - Query Execution Times by Bloom Filter Size
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `bloomSize` | Size of bloom filter in bits (500K, 1M, 2M) |
| `falsePositiveProbability` | Theoretical false positive probability for the bloom size |
| `globalScanTime` | Average time for full database scan (microseconds) |
| `hierarchicalSingleTime` | Average time for single-column hierarchical query (microseconds) |
| `hierarchicalMultiTime` | Average time for multi-column hierarchical query (microseconds) |

#### `exp_6_basic_checks.csv` - Basic Check Counts by Bloom Filter Size
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `bloomSize` | Size of bloom filter in bits |
| `multiBloomChecks` | Average bloom filter checks for multi-column queries |
| `multiLeafBloomChecks` | Average leaf bloom filter checks for multi-column queries |
| `multiSSTChecks` | Average SST file checks for multi-column queries |
| `singleBloomChecks` | Average bloom filter checks for single-column queries (raw count) |
| `singleLeafBloomChecks` | Average leaf bloom filter checks for single-column queries (raw count) |
| `singleSSTChecks` | Average SST file checks for single-column queries (raw count) |

**Note**: Single-column metrics are raw counts, not per-column normalized, since single-column queries only use one column.

---

### 2. **Per-Column Efficiency Analysis**

#### `exp_6_per_column_metrics.csv` - Multi-Column Efficiency per Bloom Size
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `bloomSize` | Size of bloom filter in bits |
| `numColumns` | Number of columns tested (3 - for reference) |
| `multiBloomPerCol` | Average bloom checks per column for multi-column queries |
| `multiLeafPerCol` | Average leaf bloom checks per column for multi-column queries |
| `multiNonLeafPerCol` | Average non-leaf bloom checks per column for multi-column queries |
| `multiSSTPerCol` | Average SST checks per column for multi-column queries |

**Key Insight**: Shows how efficiently each bloom filter size handles the work per column in multi-column scenarios.

---

### 3. **Real Data Percentage Analysis**

#### `exp_6_real_data_checks.csv` - Detailed Check Metrics by Real Data %
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `bloomSize` | Size of bloom filter in bits |
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

#### `exp_6_real_data_per_column.csv` - Per-Column Metrics by Real Data %
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `bloomSize` | Size of bloom filter in bits |
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

#### `exp_6_size_efficiency.csv` - Key Efficiency Metrics
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `bloomSize` | Size of bloom filter in bits |
| `realDataPercentage` | Percentage of queries using real data |
| `falsePositiveProbability` | Theoretical false positive probability |
| `avgMultiTime` | Average execution time for multi-column queries (microseconds) |
| `avgSingleTime` | Average execution time for single-column queries (microseconds) |
| `avgMultiBloomPerCol` | Average bloom checks per column for multi-column queries |
| `avgMultiSSTPerCol` | Average SST checks per column for multi-column queries |

**Visualization Use**: Perfect for creating charts showing how performance varies with bloom filter size across different real data percentages.

#### `exp_6_timing_comparison.csv` - Real vs False Data Performance
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `bloomSize` | Size of bloom filter in bits |
| `realDataPercentage` | Percentage of queries using real data |
| `avgRealMultiTime` | Average execution time for real data multi-column queries (microseconds) |
| `avgRealSingleTime` | Average execution time for real data single-column queries (microseconds) |
| `avgFalseMultiTime` | Average execution time for false data multi-column queries (microseconds) |
| `avgFalseSingleTime` | Average execution time for false data single-column queries (microseconds) |

**Visualization Use**: Shows false positive performance impact across different bloom filter sizes.

---

## Key Analysis Dimensions

### Bloom Filter Size Effects
- **False Positive Rate**: How bloom filter size affects theoretical and actual FPP
- **Memory vs Performance**: Trade-off between bloom filter size and query efficiency
- **Diminishing Returns**: Point where larger bloom filters provide minimal benefit

### Real Data Percentage Impact
- **False Positive Cost**: How false queries affect performance with different bloom sizes
- **Efficiency Trends**: Whether false positive impact varies with bloom filter size
- **Size Optimization**: Finding optimal bloom size for different real data ratios

### Visualization Recommendations

#### 1. **Size Efficiency Charts**
- **X-axis**: Bloom filter size (500K, 1M, 2M bits)
- **Y-axis**: Per-column metrics or timing
- **Series**: Different real data percentages
- **Data Source**: `exp_6_size_efficiency.csv`, `exp_6_per_column_metrics.csv`

#### 2. **False Positive Impact Charts**
- **X-axis**: Real data percentage (0% to 100%)
- **Y-axis**: Execution time or check counts
- **Series**: Different bloom filter sizes
- **Data Source**: `exp_6_timing_comparison.csv`, `exp_6_real_data_checks.csv`

#### 3. **Size vs Performance Comparison**
- **X-axis**: Bloom filter size
- **Y-axis**: Performance metrics
- **Series**: Multi-column vs single-column queries
- **Data Source**: `exp_6_basic_timings.csv`, `exp_6_basic_checks.csv`

#### 4. **Memory Efficiency Trade-off**
- **X-axis**: False positive probability (decreases with size)
- **Y-axis**: Per-column efficiency
- **Bubbles**: Different bloom filter sizes
- **Data Source**: `exp_6_size_efficiency.csv`

---

## Key Research Questions Answered

1. **What is the optimal bloom filter size for different workloads?**
   - Compare performance across bloom sizes for different real data percentages
   - Use `exp_6_size_efficiency.csv`

2. **How does bloom filter size affect false positive impact?**
   - Analyze performance difference between real vs false data queries
   - Use `exp_6_timing_comparison.csv`

3. **What are the memory vs performance trade-offs?**
   - Compare bloom filter sizes against query performance improvements
   - Use `exp_6_basic_timings.csv` with `exp_6_size_efficiency.csv`

4. **How does per-column efficiency change with bloom filter size?**
   - Analyze whether larger bloom filters provide better per-column efficiency
   - Use `exp_6_per_column_metrics.csv`

5. **Where are the diminishing returns for bloom filter size?**
   - Identify the point where increasing size provides minimal benefit
   - Use `exp_6_size_efficiency.csv` across all real data percentages

## Benefits for Visualization

1. **Size-Focused**: All data organized around bloom filter size as primary dimension
2. **FPP Integration**: False positive probability included for correlation analysis
3. **Raw vs Normalized**: Single-column data kept raw, multi-column data normalized per-column
4. **Efficiency Metrics**: Clear per-column efficiency data for multi-column scenarios
5. **Performance Separation**: Timing and check metrics in separate files for focused analysis

## Academic Value

This experiment provides insights into:
- **Bloom Filter Sizing**: Optimal memory allocation strategies for bloom filters
- **False Positive Management**: How bloom filter size affects false positive rates and their performance impact
- **Multi-Column Efficiency**: Scaling patterns for multi-column bloom filter hierarchies across different sizes
- **Memory-Performance Trade-offs**: Understanding the point of diminishing returns for bloom filter memory investment
- **Cost-Benefit Analysis**: Determining when larger bloom filters justify their memory overhead 