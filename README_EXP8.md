# Experiment 8 (exp8) - Column Scalability Analysis

## Overview
Experiment 8 analyzes how bloom filter performance scales with increasing column counts. It focuses on per-column metrics and real data percentage analysis to understand scalability patterns across different multi-column scenarios.

## Test Configuration
- **Database Size**: 50M records (fixed)
- **Column Counts Tested**: 4, 8, 10, 12 columns
- **Column Names**: `i_0_column`, `i_1_column`, ..., `i_11_column`
- **Bloom Filter Settings**: 1M size, 6 hash functions, 100K items per partition
- **Real Data Percentages**: 0%, 20%, 33%, 50%, 75%, 100%
- **Query Scenarios**: 100 queries per real data percentage scenario

---

## CSV Files Generated

### 1. **Basic Performance Metrics**

#### `exp_8_basic_timings.csv` - Query Execution Times by Column Count
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database (50M) |
| `numColumns` | Number of columns tested (4, 8, 10, 12) |
| `globalScanTime` | Average time for full database scan (microseconds) |
| `hierarchicalSingleTime` | Average time for single-column hierarchical query (microseconds) |
| `hierarchicalMultiTime` | Average time for multi-column hierarchical query (microseconds) |

#### `exp_8_basic_checks.csv` - Basic Check Counts by Column Count
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `numColumns` | Number of columns tested |
| `multiBloomChecks` | Average bloom filter checks for multi-column queries |
| `multiLeafBloomChecks` | Average leaf bloom filter checks for multi-column queries |
| `multiSSTChecks` | Average SST file checks for multi-column queries |
| `singleBloomChecks` | Average bloom filter checks for single-column queries |
| `singleLeafBloomChecks` | Average leaf bloom filter checks for single-column queries |
| `singleSSTChecks` | Average SST file checks for single-column queries |

---

### 2. **Per-Column Normalized Metrics**

#### `exp_8_per_column_metrics.csv` - Efficiency per Column
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `numColumns` | Number of columns tested |
| `multiBloomPerCol` | Average bloom checks per column for multi-column queries |
| `multiLeafPerCol` | Average leaf bloom checks per column for multi-column queries |
| `multiNonLeafPerCol` | Average non-leaf bloom checks per column for multi-column queries |
| `multiSSTPerCol` | Average SST checks per column for multi-column queries |
| `singleBloomPerCol` | Average bloom checks per column for single-column queries |
| `singleLeafPerCol` | Average leaf bloom checks per column for single-column queries |
| `singleNonLeafPerCol` | Average non-leaf bloom checks per column for single-column queries |
| `singleSSTPerCol` | Average SST checks per column for single-column queries |

**Key Insight**: This data shows how efficiently the system handles each additional column. Ideally, per-column metrics should remain stable regardless of total column count.

---

### 3. **Real Data Percentage Analysis**

#### `exp_8_real_data_checks.csv` - Detailed Check Metrics by Real Data %
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `numColumns` | Number of columns tested |
| `realDataPercentage` | Percentage of queries using real data (0%, 20%, 33%, 50%, 75%, 100%) |
| `avgMultiBloomChecks` | Average bloom filter checks for multi-column queries |
| `avgMultiLeafBloomChecks` | Average leaf bloom filter checks for multi-column queries |
| `avgMultiNonLeafBloomChecks` | Average non-leaf bloom filter checks for multi-column queries |
| `avgMultiSSTChecks` | Average SST file checks for multi-column queries |
| `avgSingleBloomChecks` | Average bloom filter checks for single-column queries |
| `avgSingleLeafBloomChecks` | Average leaf bloom filter checks for single-column queries |
| `avgSingleNonLeafBloomChecks` | Average non-leaf bloom filter checks for single-column queries |
| `avgSingleSSTChecks` | Average SST file checks for single-column queries |
| `avgRealMultiBloomChecks` | Average bloom filter checks for real data multi-column queries |
| `avgRealMultiSSTChecks` | Average SST file checks for real data multi-column queries |
| `avgFalseMultiBloomChecks` | Average bloom filter checks for false data multi-column queries |
| `avgFalseMultiSSTChecks` | Average SST file checks for false data multi-column queries |

#### `exp_8_real_data_per_column.csv` - Per-Column Metrics by Real Data %
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `numColumns` | Number of columns tested |
| `realDataPercentage` | Percentage of queries using real data |
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

---

### 4. **Visualization-Focused Summaries**

#### `exp_8_scalability_summary.csv` - Key Scalability Metrics
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `numColumns` | Number of columns tested |
| `realDataPercentage` | Percentage of queries using real data |
| `avgMultiTime` | Average execution time for multi-column queries (microseconds) |
| `avgSingleTime` | Average execution time for single-column queries (microseconds) |
| `avgMultiBloomPerCol` | Average bloom checks per column for multi-column queries |
| `avgMultiSSTPerCol` | Average SST checks per column for multi-column queries |

**Visualization Use**: Perfect for creating charts showing how performance scales with column count across different real data percentages.

#### `exp_8_timing_comparison.csv` - Real vs False Data Performance
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `numColumns` | Number of columns tested |
| `realDataPercentage` | Percentage of queries using real data |
| `avgRealMultiTime` | Average execution time for real data multi-column queries (microseconds) |
| `avgRealSingleTime` | Average execution time for real data single-column queries (microseconds) |
| `avgFalseMultiTime` | Average execution time for false data multi-column queries (microseconds) |
| `avgFalseSingleTime` | Average execution time for false data single-column queries (microseconds) |

**Visualization Use**: Shows performance impact of false positives across different column counts.

---

## Key Analysis Dimensions

### Column Scalability Patterns
- **Linear Scaling**: Check if metrics increase linearly with column count
- **Per-Column Efficiency**: Whether efficiency per column remains constant
- **Multi-Column Overhead**: Additional cost of coordinating multiple hierarchies

### Real Data Percentage Impact
- **False Positive Cost**: How false queries affect performance at different scales
- **Efficiency Degradation**: Whether false positive impact increases with column count
- **Optimization Opportunities**: Identifying best real data percentage thresholds

### Visualization Recommendations

#### 1. **Scalability Charts**
- **X-axis**: Number of columns (4, 8, 10, 12)
- **Y-axis**: Per-column metrics (bloom checks, SST checks, timing)
- **Series**: Different real data percentages
- **Data Source**: `exp_8_scalability_summary.csv`, `exp_8_real_data_per_column.csv`

#### 2. **Performance Comparison Charts**
- **X-axis**: Real data percentage (0% to 100%)
- **Y-axis**: Execution time or check counts
- **Series**: Different column counts
- **Data Source**: `exp_8_timing_comparison.csv`, `exp_8_real_data_checks.csv`

#### 3. **Efficiency Heatmaps**
- **X-axis**: Number of columns
- **Y-axis**: Real data percentage  
- **Color**: Per-column efficiency metrics
- **Data Source**: `exp_8_real_data_per_column.csv`

#### 4. **Cost Breakdown Charts**
- **Stacked bars**: Bloom checks (leaf vs non-leaf) and SST checks
- **Categories**: Different column counts
- **Data Source**: `exp_8_basic_checks.csv`, `exp_8_per_column_metrics.csv`

---

## Key Research Questions Answered

1. **Does performance scale linearly with column count?**
   - Compare per-column metrics across different column counts
   - Use `exp_8_per_column_metrics.csv`

2. **How does false positive impact change with scale?**
   - Analyze timing differences between real vs false data
   - Use `exp_8_timing_comparison.csv`

3. **What is the optimal real data percentage for different scales?**
   - Find performance sweet spots across column counts
   - Use `exp_8_scalability_summary.csv`

4. **Where are the bottlenecks in multi-column queries?**
   - Compare bloom vs SST check ratios
   - Use `exp_8_real_data_checks.csv`

## Benefits for Visualization

1. **Focused Data**: Each CSV targets specific visualization needs
2. **Consistent Dimensions**: Column count and real data percentage as primary axes
3. **Normalized Metrics**: Per-column data enables fair comparisons
4. **Multiple Granularities**: Summary files for overviews, detailed files for deep analysis
5. **Performance Separated**: Timing and check metrics in separate files for cleaner charts 