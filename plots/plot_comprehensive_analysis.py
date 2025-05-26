#!/usr/bin/env python3
"""
Script to plot data from exp_8_comprehensive_analysis.csv
Analyzes performance differences between real and false data queries
across different real data percentages and number of columns
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

def load_comprehensive_data(file_path):
    """Load and clean comprehensive analysis CSV data"""
    
    if not Path(file_path).exists():
        print(f"Error: {file_path} not found!")
        return None
    
    df = pd.read_csv(file_path)
    
    # Remove any rows with missing data
    df = df.dropna()
    
    print(f"Loaded {len(df)} data points")
    print(f"Columns: {df['NumColumns'].unique()}")
    print(f"Real data percentages: {sorted(df['RealDataPercentage'].unique())}")
    
    return df

def plot_query_times_by_percentage(df):
    """Plot query times by real data percentage - separate plots for multi and single"""
    
    unique_columns = sorted(df['NumColumns'].unique())
    colors = plt.cm.Set1(np.linspace(0, 1, len(unique_columns)))
    
    # Plot 1: Multi-Column Time
    plt.figure(figsize=(12, 8))
    for i, num_cols in enumerate(unique_columns):
        subset = df[df['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgHierarchicalMultiTime'], 
                'o-', color=colors[i], label=f'{num_cols} columns', linewidth=2, markersize=6)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Avg Multi-Column Time (μs)')
    plt.title('Multi-Column Query Performance vs Real Data Percentage')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig('01_multi_column_time_by_percentage.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 2: Single-Column Time
    plt.figure(figsize=(12, 8))
    for i, num_cols in enumerate(unique_columns):
        subset = df[df['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgHierarchicalSingleTime'], 
                'o-', color=colors[i], label=f'{num_cols} columns', linewidth=2, markersize=6)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Avg Single-Column Time (μs)')
    plt.title('Single-Column Query Performance vs Real Data Percentage')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('02_single_column_time_by_percentage.png', dpi=300, bbox_inches='tight')
    plt.close()

def plot_bloom_checks_by_percentage(df):
    """Plot bloom filter checks by real data percentage - separate multi vs single"""
    
    unique_columns = sorted(df['NumColumns'].unique())
    colors = plt.cm.Set1(np.linspace(0, 1, len(unique_columns)))
    
    # Plot 1: Multi-Column Bloom Checks
    plt.figure(figsize=(12, 8))
    for i, num_cols in enumerate(unique_columns):
        subset = df[df['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgMultiBloomChecks'], 
                'o-', color=colors[i], label=f'{num_cols} columns', linewidth=2, markersize=6)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Avg Multi-Column Bloom Checks')
    plt.title('Multi-Column Bloom Filter Checks vs Real Data Percentage')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('03_multi_bloom_checks_by_percentage.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 2: Single-Column Bloom Checks
    plt.figure(figsize=(12, 8))
    for i, num_cols in enumerate(unique_columns):
        subset = df[df['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgSingleBloomChecks'], 
                'o-', color=colors[i], label=f'{num_cols} columns', linewidth=2, markersize=6)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Avg Single-Column Bloom Checks')
    plt.title('Single-Column Bloom Filter Checks vs Real Data Percentage')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('04_single_bloom_checks_by_percentage.png', dpi=300, bbox_inches='tight')
    plt.close()

def plot_sst_checks_by_percentage(df):
    """Plot SST checks by real data percentage - separate multi vs single"""
    
    unique_columns = sorted(df['NumColumns'].unique())
    colors = plt.cm.Set1(np.linspace(0, 1, len(unique_columns)))
    
    # Plot 1: Multi-Column SST Checks
    plt.figure(figsize=(12, 8))
    for i, num_cols in enumerate(unique_columns):
        subset = df[df['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgMultiSSTChecks'], 
                'o-', color=colors[i], label=f'{num_cols} columns', linewidth=2, markersize=6)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Avg Multi-Column SST Checks')
    plt.title('Multi-Column SST Checks vs Real Data Percentage')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('05_multi_sst_checks_by_percentage.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 2: Single-Column SST Checks
    plt.figure(figsize=(12, 8))
    for i, num_cols in enumerate(unique_columns):
        subset = df[df['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgSingleSSTChecks'], 
                'o-', color=colors[i], label=f'{num_cols} columns', linewidth=2, markersize=6)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Avg Single-Column SST Checks')
    plt.title('Single-Column SST Checks vs Real Data Percentage')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('06_single_sst_checks_by_percentage.png', dpi=300, bbox_inches='tight')
    plt.close()

def plot_real_vs_false_comparison(df):
    """Plot direct comparison between real and false data performance"""
    
    # Filter out 0% and 100% data for meaningful comparison
    df_filtered = df[(df['RealDataPercentage'] > 0) & (df['RealDataPercentage'] < 100)].copy()
    
    unique_columns = sorted(df_filtered['NumColumns'].unique())
    colors = plt.cm.Set1(np.linspace(0, 1, len(unique_columns)))
    
    # Plot 1: Real vs False Multi-Column Time
    plt.figure(figsize=(12, 8))
    for i, num_cols in enumerate(unique_columns):
        subset = df_filtered[df_filtered['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgRealDataMultiTime'], 
                'o-', color=colors[i], label=f'{num_cols} cols (Real)', linewidth=2, markersize=6)
        plt.plot(subset['RealDataPercentage'], subset['AvgFalseDataMultiTime'], 
                's--', color=colors[i], label=f'{num_cols} cols (False)', linewidth=2, markersize=6, alpha=0.7)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Query Time (μs)')
    plt.title('Real vs False Data: Multi-Column Query Time')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig('07_real_vs_false_multi_time.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 2: Real vs False Single-Column Time
    plt.figure(figsize=(12, 8))
    for i, num_cols in enumerate(unique_columns):
        subset = df_filtered[df_filtered['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgRealDataSingleTime'], 
                'o-', color=colors[i], label=f'{num_cols} cols (Real)', linewidth=2, markersize=6)
        plt.plot(subset['RealDataPercentage'], subset['AvgFalseDataSingleTime'], 
                's--', color=colors[i], label=f'{num_cols} cols (False)', linewidth=2, markersize=6, alpha=0.7)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Query Time (μs)')
    plt.title('Real vs False Data: Single-Column Query Time')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('08_real_vs_false_single_time.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 3: Real vs False Multi-Column Bloom Checks
    plt.figure(figsize=(12, 8))
    for i, num_cols in enumerate(unique_columns):
        subset = df_filtered[df_filtered['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgRealMultiBloomChecks'], 
                'o-', color=colors[i], label=f'{num_cols} cols (Real)', linewidth=2, markersize=6)
        plt.plot(subset['RealDataPercentage'], subset['AvgFalseMultiBloomChecks'], 
                's--', color=colors[i], label=f'{num_cols} cols (False)', linewidth=2, markersize=6, alpha=0.7)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Multi-Column Bloom Checks')
    plt.title('Real vs False Data: Multi-Column Bloom Filter Checks')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('09_real_vs_false_multi_bloom.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 4: Real vs False Multi-Column SST Checks
    plt.figure(figsize=(12, 8))
    for i, num_cols in enumerate(unique_columns):
        subset = df_filtered[df_filtered['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgRealMultiSSTChecks'], 
                'o-', color=colors[i], label=f'{num_cols} cols (Real)', linewidth=2, markersize=6)
        plt.plot(subset['RealDataPercentage'], subset['AvgFalseMultiSSTChecks'], 
                's--', color=colors[i], label=f'{num_cols} cols (False)', linewidth=2, markersize=6, alpha=0.7)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Multi-Column SST Checks')
    plt.title('Real vs False Data: Multi-Column SST Checks')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('10_real_vs_false_multi_sst.png', dpi=300, bbox_inches='tight')
    plt.close()

def plot_efficiency_heatmaps(df):
    """Create heatmaps showing efficiency across columns and percentages"""
    
    # Prepare data for heatmaps
    pivot_multi_time = df.pivot(index='NumColumns', columns='RealDataPercentage', values='AvgHierarchicalMultiTime')
    pivot_single_time = df.pivot(index='NumColumns', columns='RealDataPercentage', values='AvgHierarchicalSingleTime')
    pivot_multi_bloom = df.pivot(index='NumColumns', columns='RealDataPercentage', values='AvgMultiBloomChecks')
    pivot_single_bloom = df.pivot(index='NumColumns', columns='RealDataPercentage', values='AvgSingleBloomChecks')
    pivot_multi_sst = df.pivot(index='NumColumns', columns='RealDataPercentage', values='AvgMultiSSTChecks')
    pivot_single_sst = df.pivot(index='NumColumns', columns='RealDataPercentage', values='AvgSingleSSTChecks')
    
    # Heatmap 1: Multi-Column Time
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_multi_time, annot=True, fmt='.0f', cmap='YlOrRd', cbar_kws={'label': 'Time (μs)'})
    plt.title('Multi-Column Query Time Heatmap')
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Number of Columns')
    plt.tight_layout()
    plt.savefig('11_heatmap_multi_time.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Heatmap 2: Single-Column Time
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_single_time, annot=True, fmt='.0f', cmap='YlGnBu', cbar_kws={'label': 'Time (μs)'})
    plt.title('Single-Column Query Time Heatmap')
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Number of Columns')
    plt.tight_layout()
    plt.savefig('12_heatmap_single_time.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Heatmap 3: Multi-Column Bloom Checks
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_multi_bloom, annot=True, fmt='.1f', cmap='Purples', cbar_kws={'label': 'Checks'})
    plt.title('Multi-Column Bloom Filter Checks Heatmap')
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Number of Columns')
    plt.tight_layout()
    plt.savefig('13_heatmap_multi_bloom.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Heatmap 4: Single-Column Bloom Checks
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_single_bloom, annot=True, fmt='.1f', cmap='Purples', cbar_kws={'label': 'Checks'})
    plt.title('Single-Column Bloom Filter Checks Heatmap')
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Number of Columns')
    plt.tight_layout()
    plt.savefig('14_heatmap_single_bloom.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Heatmap 5: Multi-Column SST Checks
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_multi_sst, annot=True, fmt='.1f', cmap='Greens', cbar_kws={'label': 'Checks'})
    plt.title('Multi-Column SST Checks Heatmap')
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Number of Columns')
    plt.tight_layout()
    plt.savefig('15_heatmap_multi_sst.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Heatmap 6: Single-Column SST Checks
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_single_sst, annot=True, fmt='.1f', cmap='Greens', cbar_kws={'label': 'Checks'})
    plt.title('Single-Column SST Checks Heatmap')
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Number of Columns')
    plt.tight_layout()
    plt.savefig('16_heatmap_single_sst.png', dpi=300, bbox_inches='tight')
    plt.close()

def plot_scalability_analysis(df):
    """Analyze scalability with number of columns"""
    
    # Get data for specific percentages
    percentages_to_plot = [0, 50, 100]
    colors = ['red', 'blue', 'green']
    
    # Plot 1: Multi-Column Time Scalability
    plt.figure(figsize=(12, 8))
    for i, pct in enumerate(percentages_to_plot):
        subset = df[df['RealDataPercentage'] == pct]
        plt.plot(subset['NumColumns'], subset['AvgHierarchicalMultiTime'], 
                'o-', color=colors[i], label=f'{pct}% real data', linewidth=2, markersize=8)
    
    plt.xlabel('Number of Columns')
    plt.ylabel('Avg Multi-Column Time (μs)')
    plt.title('Multi-Column Time Scalability')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig('17_scalability_multi_time.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 2: Single-Column Time Scalability
    plt.figure(figsize=(12, 8))
    for i, pct in enumerate(percentages_to_plot):
        subset = df[df['RealDataPercentage'] == pct]
        plt.plot(subset['NumColumns'], subset['AvgHierarchicalSingleTime'], 
                'o-', color=colors[i], label=f'{pct}% real data', linewidth=2, markersize=8)
    
    plt.xlabel('Number of Columns')
    plt.ylabel('Avg Single-Column Time (μs)')
    plt.title('Single-Column Time Scalability')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('18_scalability_single_time.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 3: Multi-Column Bloom Checks Scalability
    plt.figure(figsize=(12, 8))
    for i, pct in enumerate(percentages_to_plot):
        subset = df[df['RealDataPercentage'] == pct]
        plt.plot(subset['NumColumns'], subset['AvgMultiBloomChecks'], 
                'o-', color=colors[i], label=f'{pct}% real data', linewidth=2, markersize=8)
    
    plt.xlabel('Number of Columns')
    plt.ylabel('Avg Multi-Column Bloom Checks')
    plt.title('Multi-Column Bloom Filter Checks Scalability')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('19_scalability_multi_bloom.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 4: Single-Column Bloom Checks Scalability
    plt.figure(figsize=(12, 8))
    for i, pct in enumerate(percentages_to_plot):
        subset = df[df['RealDataPercentage'] == pct]
        plt.plot(subset['NumColumns'], subset['AvgSingleBloomChecks'], 
                'o-', color=colors[i], label=f'{pct}% real data', linewidth=2, markersize=8)
    
    plt.xlabel('Number of Columns')
    plt.ylabel('Avg Single-Column Bloom Checks')
    plt.title('Single-Column Bloom Filter Checks Scalability')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('20_scalability_single_bloom.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 5: Multi-Column SST Checks Scalability
    plt.figure(figsize=(12, 8))
    for i, pct in enumerate(percentages_to_plot):
        subset = df[df['RealDataPercentage'] == pct]
        plt.plot(subset['NumColumns'], subset['AvgMultiSSTChecks'], 
                'o-', color=colors[i], label=f'{pct}% real data', linewidth=2, markersize=8)
    
    plt.xlabel('Number of Columns')
    plt.ylabel('Avg Multi-Column SST Checks')
    plt.title('Multi-Column SST Checks Scalability')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('21_scalability_multi_sst.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 6: Single-Column SST Checks Scalability
    plt.figure(figsize=(12, 8))
    for i, pct in enumerate(percentages_to_plot):
        subset = df[df['RealDataPercentage'] == pct]
        plt.plot(subset['NumColumns'], subset['AvgSingleSSTChecks'], 
                'o-', color=colors[i], label=f'{pct}% real data', linewidth=2, markersize=8)
    
    plt.xlabel('Number of Columns')
    plt.ylabel('Avg Single-Column SST Checks')
    plt.title('Single-Column SST Checks Scalability')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('22_scalability_single_sst.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot 7: Efficiency Ratio (Multi vs Single Time)
    plt.figure(figsize=(12, 8))
    for i, pct in enumerate(percentages_to_plot):
        subset = df[df['RealDataPercentage'] == pct]
        ratio = subset['AvgHierarchicalMultiTime'] / subset['AvgHierarchicalSingleTime']
        plt.plot(subset['NumColumns'], ratio, 
                'o-', color=colors[i], label=f'{pct}% real data', linewidth=2, markersize=8)
    
    plt.xlabel('Number of Columns')
    plt.ylabel('Multi/Single Time Ratio')
    plt.title('Query Efficiency Ratio (Multi vs Single Column)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.axhline(y=1, color='black', linestyle='--', alpha=0.5, label='Equal performance')
    plt.tight_layout()
    plt.savefig('23_efficiency_ratio.png', dpi=300, bbox_inches='tight')
    plt.close()

def print_comprehensive_summary(df):
    """Print comprehensive summary statistics"""
    
    print("=== COMPREHENSIVE ANALYSIS SUMMARY ===")
    print(f"Total experiments: {len(df)}")
    print(f"Number of columns tested: {sorted(df['NumColumns'].unique())}")
    print(f"Real data percentages: {sorted(df['RealDataPercentage'].unique())}")
    print(f"Records per experiment: {df['NumRecords'].iloc[0]:,}")
    print()
    
    # Performance at extremes
    all_false = df[df['RealDataPercentage'] == 0]
    all_true = df[df['RealDataPercentage'] == 100]
    
    print("Performance at Extremes:")
    print("0% Real Data (All False):")
    print(f"  Multi-Column Time: {all_false['AvgHierarchicalMultiTime'].mean():.1f} μs (avg)")
    print(f"  Bloom Checks: {all_false['AvgMultiBloomChecks'].mean():.1f} (avg)")
    print(f"  SST Checks: {all_false['AvgMultiSSTChecks'].mean():.1f} (avg)")
    print()
    
    print("100% Real Data (All True):")
    print(f"  Multi-Column Time: {all_true['AvgHierarchicalMultiTime'].mean():.1f} μs (avg)")
    print(f"  Bloom Checks: {all_true['AvgMultiBloomChecks'].mean():.1f} (avg)")
    print(f"  SST Checks: {all_true['AvgMultiSSTChecks'].mean():.1f} (avg)")
    print()
    
    # Mixed data performance
    mixed = df[(df['RealDataPercentage'] > 0) & (df['RealDataPercentage'] < 100)]
    if len(mixed) > 0:
        print("Mixed Data Performance (Real vs False):")
        print(f"  Real Data Avg Time: {mixed['AvgRealDataMultiTime'].mean():.1f} μs")
        print(f"  False Data Avg Time: {mixed['AvgFalseDataMultiTime'].mean():.1f} μs")
        print(f"  Performance Ratio (Real/False): {(mixed['AvgRealDataMultiTime'].mean() / mixed['AvgFalseDataMultiTime'].mean()):.1f}x")

def main():
    """Main function to run all plotting functions"""
    
    # Set style
    plt.style.use('seaborn-v0_8')
    sns.set_palette("husl")
    
    # Load data
    csv_file = '../50m_csv/exp_8_comprehensive_analysis.csv'
    df = load_comprehensive_data(csv_file)
    
    if df is None:
        return
    
    # Print summary
    print_comprehensive_summary(df)
    print()
    
    # Generate plots
    print("Generating query time plots...")
    plot_query_times_by_percentage(df)
    
    print("Generating bloom filter check plots...")
    plot_bloom_checks_by_percentage(df)
    
    print("Generating SST check plots...")
    plot_sst_checks_by_percentage(df)
    
    print("Generating real vs false comparison plots...")
    plot_real_vs_false_comparison(df)
    
    print("Generating efficiency heatmaps...")
    plot_efficiency_heatmaps(df)
    
    print("Generating scalability analysis...")
    plot_scalability_analysis(df)
    
    print("All plots saved as PNG files!")
    print("Files created:")
    print("  01_multi_column_time_by_percentage.png")
    print("  02_single_column_time_by_percentage.png")
    print("  03_multi_bloom_checks_by_percentage.png")
    print("  04_single_bloom_checks_by_percentage.png")
    print("  05_multi_sst_checks_by_percentage.png")
    print("  06_single_sst_checks_by_percentage.png")
    print("  07_real_vs_false_multi_time.png")
    print("  08_real_vs_false_single_time.png")
    print("  09_real_vs_false_multi_bloom.png")
    print("  10_real_vs_false_multi_sst.png")
    print("  11_heatmap_multi_time.png")
    print("  12_heatmap_single_time.png")
    print("  13_heatmap_multi_bloom.png")
    print("  14_heatmap_single_bloom.png")
    print("  15_heatmap_multi_sst.png")
    print("  16_heatmap_single_sst.png")
    print("  17_scalability_multi_time.png")
    print("  18_scalability_single_time.png")
    print("  19_scalability_multi_bloom.png")
    print("  20_scalability_single_bloom.png")
    print("  21_scalability_multi_sst.png")
    print("  22_scalability_single_sst.png")
    print("  23_efficiency_ratio.png")

if __name__ == "__main__":
    main() 