# Experiment 7 (exp7) - Query Selectivity & Targeted Record Analysis

## Overview
Experiment 7 analyzes bloom filter performance for targeted queries where specific records are known to exist in the database. Unlike other experiments that test random queries, exp7 modifies specific records to target values and then queries for those exact values, measuring how bloom filter efficiency changes with query selectivity.

## Test Configuration
- **Database Size**: Configurable (typically 50M records)
- **Columns Tested**: `phone`, `mail`, `address` (3 columns, fixed)
- **Target Record Counts**: 2, 4, 6, 8, 10 records
- **Bloom Filter Settings**: 1M size, 6 hash functions, 100K items per partition (fixed)
- **Query Type**: Targeted queries for known existing records
- **Modification Strategy**: Changes selected records to `{column}_target` values

---

## Experimental Methodology

### **Unique Approach: Targeted Query Testing**
1. **Record Selection**: Randomly selects N records from the database
2. **Value Modification**: Changes all columns of selected records to `{column}_target` format
3. **Targeted Querying**: Queries specifically for the modified target values
4. **Performance Measurement**: Measures bloom filter efficiency for known existing data
5. **Selectivity Analysis**: Compares performance across different numbers of target records

### **Key Difference from Other Experiments**
- **Other Experiments**: Test random queries (mix of existing/non-existing values)
- **Exp7**: Tests targeted queries for guaranteed existing records
- **Focus**: Query selectivity impact on bloom filter performance

---

## CSV Files Generated

### 1. **Raw Performance Metrics**

#### `exp_7_checks.csv` - Basic Check Counts by Target Record Count
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `keys` | Number of target records modified and queried (2, 4, 6, 8, 10) |
| `sstFiles` | Total number of SST files across all columns |
| `multiCol_bloomChecks_avg/min/max` | Multi-column bloom filter check statistics |
| `multiCol_leafBloomChecks_avg/min/max` | Multi-column leaf bloom filter check statistics |
| `multiCol_sstChecks_avg/min/max` | Multi-column SST file check statistics |
| `singleCol_bloomChecks_avg/min/max` | Single-column bloom filter check statistics (raw count) |
| `singleCol_leafBloomChecks_avg/min/max` | Single-column leaf bloom filter check statistics (raw count) |
| `singleCol_sstChecks_avg/min/max` | Single-column SST file check statistics (raw count) |

**Note**: Single-column metrics are raw counts since single-column queries don't scale with column count.

#### `exp_7_derived_metrics.csv` - Non-Leaf Bloom Filter Analysis
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `keys` | Number of target records |
| `sstFiles` | Total number of SST files |
| `multiCol_nonLeafBloomChecks_avg/min/max` | Multi-column non-leaf bloom checks (bloom - leaf) |
| `singleCol_nonLeafBloomChecks_avg/min/max` | Single-column non-leaf bloom checks (bloom - leaf) |

**Key Insight**: Shows internal vs leaf bloom filter usage patterns for targeted queries.

---

### 2. **Efficiency Analysis**

#### `exp_7_per_column.csv` - Multi-Column Efficiency per Target Count
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `keys` | Number of target records |
| `sstFiles` | Total number of SST files |
| `numColumns` | Number of columns tested (3 - for reference) |
| `multiCol_bloomChecksPerColumn_avg/min/max` | Average bloom checks per column for multi-column queries |
| `multiCol_leafBloomChecksPerColumn_avg/min/max` | Average leaf bloom checks per column for multi-column queries |
| `multiCol_nonLeafBloomChecksPerColumn_avg/min/max` | Average non-leaf bloom checks per column for multi-column queries |
| `multiCol_sstChecksPerColumn_avg/min/max` | Average SST checks per column for multi-column queries |

**Key Insight**: Shows how efficiently each column handles targeted queries as the number of target records increases.

---

### 3. **Performance Summaries**

#### `exp_7_timings.csv` - Query Execution Times
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `keys` | Number of target records |
| `hierarchicalSingleTime_avg/min/max` | Single-column hierarchical query time statistics (microseconds) |
| `hierarchicalMultiTime_avg/min/max` | Multi-column hierarchical query time statistics (microseconds) |

#### `exp_7_overview.csv` - Performance Overview
| Column | Description |
|--------|-------------|
| `numRecords` | Number of records in the database |
| `keys` | Number of target records |
| `falsePositiveProbability` | Theoretical false positive probability |
| `globalScanTime_avg` | Average time for full database scan (microseconds) |
| `hierarchicalSingleTime_avg` | Average time for single-column hierarchical query (microseconds) |
| `hierarchicalMultiTime_avg` | Average time for multi-column hierarchical query (microseconds) |

#### `exp_7_selected_avg_checks.csv` - Key Averages Summary
| Column | Description |
|--------|-------------|
| `numRec` | Number of records in the database |
| `keys` | Number of target records |
| `mcBloomAvg` | Average multi-column bloom checks |
| `mcLeafAvg` | Average multi-column leaf bloom checks |
| `mcNonLeafAvg` | Average multi-column non-leaf bloom checks |
| `mcSSTAvg` | Average multi-column SST checks |
| `scBloomAvg` | Average single-column bloom checks |
| `scLeafAvg` | Average single-column leaf bloom checks |
| `scNonLeafAvg` | Average single-column non-leaf bloom checks |
| `scSSTAvg` | Average single-column SST checks |

---

## Key Analysis Dimensions

### Query Selectivity Impact
- **Low Selectivity**: 2 target records - minimal bloom filter pressure
- **Medium Selectivity**: 4-6 target records - moderate bloom filter usage
- **High Selectivity**: 8-10 target records - higher bloom filter pressure
- **Efficiency Scaling**: How performance changes with increasing target count

### Targeted vs Random Query Performance
- **Guaranteed Hits**: All queries find existing records (100% hit rate)
- **Bloom Filter Effectiveness**: Measures bloom filter efficiency for positive cases
- **SST File Access Patterns**: How target record distribution affects SST access

### Visualization Recommendations

#### 1. **Selectivity Scaling Charts**
- **X-axis**: Number of target records (2, 4, 6, 8, 10)
- **Y-axis**: Per-column checks or timing
- **Series**: Multi-column vs single-column queries
- **Data Source**: `exp_7_per_column.csv`, `exp_7_timings.csv`

#### 2. **Efficiency vs Selectivity**
- **X-axis**: Number of target records
- **Y-axis**: Bloom filter checks per column
- **Series**: Leaf vs non-leaf bloom checks
- **Data Source**: `exp_7_per_column.csv`

#### 3. **Performance Scaling Analysis**
- **X-axis**: Target record count
- **Y-axis**: Query execution time
- **Series**: Single vs multi-column approaches
- **Data Source**: `exp_7_timings.csv`

#### 4. **Bloom Filter Pressure**
- **X-axis**: Number of target records
- **Y-axis**: Total bloom filter checks
- **Comparison**: Multi-column vs single-column efficiency
- **Data Source**: `exp_7_checks.csv`

---

## Key Research Questions Answered

1. **How does query selectivity affect bloom filter performance?**
   - Compare performance across different target record counts
   - Use `exp_7_per_column.csv` and `exp_7_timings.csv`

2. **What is the optimal approach for targeted queries?**
   - Analyze single-column vs multi-column efficiency for known records
   - Use `exp_7_selected_avg_checks.csv`

3. **How do bloom filters handle guaranteed positive cases?**
   - Examine bloom filter pressure when all queries hit existing records
   - Use `exp_7_checks.csv` and `exp_7_derived_metrics.csv`

4. **Where are the performance bottlenecks for targeted queries?**
   - Identify whether bloom filters or SST access dominate
   - Use `exp_7_checks.csv` with bloom vs SST check ratios

5. **How does per-column efficiency change with selectivity?**
   - Analyze whether more target records provide better per-column efficiency
   - Use `exp_7_per_column.csv`

## Benefits for Visualization

1. **Selectivity-Focused**: All data organized around target record count as primary dimension
2. **Guaranteed Hits**: Eliminates false negative variability, focuses on bloom filter efficiency
3. **Scaling Analysis**: Clear progression from low to high selectivity scenarios
4. **Multi-Dimensional**: Separate files for timing, efficiency, and raw metrics
5. **Targeted Insights**: Specifically designed for understanding known-record query patterns

## Academic Value

This experiment provides unique insights into:
- **Query Selectivity Impact**: How the number of target records affects bloom filter performance
- **Positive Case Analysis**: Bloom filter behavior when queries are guaranteed to find records
- **Targeted Query Optimization**: Optimal strategies for searching known existing records
- **Multi-Column Coordination**: How multiple bloom filters coordinate for targeted multi-column queries
- **Real-World Applicability**: Performance patterns for applications that frequently query known record sets

## Use Cases

This experiment is particularly relevant for:
- **Cache Warming**: Applications that pre-load known record sets
- **Batch Processing**: Systems that process specific record batches
- **Audit Queries**: Systems that frequently query known existing records
- **Update Operations**: Applications that modify and then re-query specific records
- **Targeted Analytics**: Systems that analyze specific known data subsets 