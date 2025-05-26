#!/usr/bin/env python3
"""
Quick analysis script for exp_8_comprehensive_analysis.csv
Provides simple functions for custom analysis and plotting
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def load_data():
    """Load the comprehensive analysis data"""
    return pd.read_csv('50m_csv/exp_8_comprehensive_analysis.csv')

def show_summary(df):
    """Show basic summary of the data"""
    print("=== QUICK SUMMARY ===")
    print(f"Data shape: {df.shape}")
    print(f"Columns tested: {sorted(df['NumColumns'].unique())}")
    print(f"Real data %: {sorted(df['RealDataPercentage'].unique())}")
    print()
    
    # Show performance difference between 0% and 100% real data
    false_data = df[df['RealDataPercentage'] == 0]['AvgHierarchicalMultiTime'].mean()
    real_data = df[df['RealDataPercentage'] == 100]['AvgHierarchicalMultiTime'].mean()
    
    print(f"Avg time with 0% real data (all false): {false_data:.1f} μs")
    print(f"Avg time with 100% real data (all true): {real_data:.1f} μs")
    print(f"Performance difference: {real_data/false_data:.1f}x slower for real data")

def plot_simple_comparison(df, num_columns=None):
    """Simple plot comparing real vs false data performance"""
    
    if num_columns:
        df = df[df['NumColumns'] == num_columns]
        title_suffix = f" ({num_columns} columns)"
    else:
        title_suffix = " (all columns)"
    
    plt.figure(figsize=(12, 8))
    
    # Plot 1: Multi-column time by percentage
    plt.subplot(2, 2, 1)
    for num_cols in sorted(df['NumColumns'].unique()):
        subset = df[df['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgHierarchicalMultiTime'], 
                'o-', label=f'{num_cols} cols', linewidth=2, markersize=6)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Multi-Column Time (μs)')
    plt.title(f'Query Time vs Real Data %{title_suffix}')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.yscale('log')
    
    # Plot 2: Bloom checks
    plt.subplot(2, 2, 2)
    for num_cols in sorted(df['NumColumns'].unique()):
        subset = df[df['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgMultiBloomChecks'], 
                'o-', label=f'{num_cols} cols', linewidth=2, markersize=6)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Bloom Checks')
    plt.title(f'Bloom Checks vs Real Data %{title_suffix}')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Plot 3: SST checks
    plt.subplot(2, 2, 3)
    for num_cols in sorted(df['NumColumns'].unique()):
        subset = df[df['NumColumns'] == num_cols]
        plt.plot(subset['RealDataPercentage'], subset['AvgMultiSSTChecks'], 
                'o-', label=f'{num_cols} cols', linewidth=2, markersize=6)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('SST Checks')
    plt.title(f'SST Checks vs Real Data %{title_suffix}')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Plot 4: Real vs False comparison (for mixed scenarios)
    plt.subplot(2, 2, 4)
    mixed_data = df[(df['RealDataPercentage'] > 0) & (df['RealDataPercentage'] < 100)]
    if len(mixed_data) > 0:
        for num_cols in sorted(mixed_data['NumColumns'].unique()):
            subset = mixed_data[mixed_data['NumColumns'] == num_cols]
            plt.plot(subset['RealDataPercentage'], subset['AvgRealDataMultiTime'], 
                    'o-', label=f'{num_cols} cols (Real)', linewidth=2, markersize=6)
            plt.plot(subset['RealDataPercentage'], subset['AvgFalseDataMultiTime'], 
                    's--', label=f'{num_cols} cols (False)', linewidth=2, markersize=6, alpha=0.7)
    
    plt.xlabel('Real Data Percentage (%)')
    plt.ylabel('Query Time (μs)')
    plt.title(f'Real vs False Data Time{title_suffix}')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.yscale('log')
    
    plt.tight_layout()
    plt.savefig(f'quick_analysis{title_suffix.replace(" ", "_").replace("(", "").replace(")", "")}.png', 
                dpi=300, bbox_inches='tight')
    plt.show()

def analyze_bloom_effectiveness(df):
    """Analyze bloom filter effectiveness"""
    print("=== BLOOM FILTER EFFECTIVENESS ===")
    
    # Compare bloom checks vs SST checks
    for num_cols in sorted(df['NumColumns'].unique()):
        subset = df[df['NumColumns'] == num_cols]
        
        print(f"\n{num_cols} Columns:")
        for _, row in subset.iterrows():
            pct = row['RealDataPercentage']
            bloom_checks = row['AvgMultiBloomChecks']
            sst_checks = row['AvgMultiSSTChecks']
            
            if sst_checks > 0:
                effectiveness = bloom_checks / sst_checks
                print(f"  {pct:3.0f}% real data: {bloom_checks:6.1f} bloom checks, {sst_checks:4.1f} SST checks (ratio: {effectiveness:.1f}:1)")
            else:
                print(f"  {pct:3.0f}% real data: {bloom_checks:6.1f} bloom checks, {sst_checks:4.1f} SST checks (no SST access)")

def main():
    """Main function for quick analysis"""
    
    # Load data
    df = load_data()
    
    # Show summary
    show_summary(df)
    print()
    
    # Analyze bloom filter effectiveness
    analyze_bloom_effectiveness(df)
    print()
    
    # Generate plots
    print("Generating quick analysis plots...")
    plot_simple_comparison(df)
    
    # Generate plots for specific column counts
    for num_cols in [2, 3, 4, 5]:
        if num_cols in df['NumColumns'].values:
            plot_simple_comparison(df, num_cols)
    
    print("Quick analysis complete!")

if __name__ == "__main__":
    main() 