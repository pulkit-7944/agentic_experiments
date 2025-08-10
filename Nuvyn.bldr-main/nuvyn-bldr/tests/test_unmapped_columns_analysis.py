#!/usr/bin/env python3
"""
Comprehensive Unmapped Columns Analysis Test for Nuvyn.bldr

This test automatically analyzes all datasets in the data folder and generates
detailed reports on unmapped columns in STTM files. It's designed to be scalable
and will automatically detect new datasets without manual configuration.

Features:
- Automatic dataset detection
- Comprehensive column mapping analysis
- Detailed reporting with coverage statistics
- Export capabilities (JSON, CSV, HTML)
- Configurable analysis options
- Runs as a proper test with assertions
"""

import os
import json
import pandas as pd
import pytest
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from datetime import datetime
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

class UnmappedColumnsAnalyzer:
    """Comprehensive analyzer for unmapped columns in STTM files."""
    
    def __init__(self, data_dir: str = "data", output_dir: str = "output"):
        """
        Initialize the analyzer.
        
        Args:
            data_dir: Directory containing datasets
            output_dir: Directory containing STTM output files
        """
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.results = {}
        
    def discover_datasets(self) -> List[str]:
        """
        Automatically discover all datasets in the data directory.
        
        Returns:
            List of dataset directory names
        """
        if not self.data_dir.exists():
            print(f"❌ Data directory not found: {self.data_dir}")
            return []
            
        datasets = []
        for item in self.data_dir.iterdir():
            if item.is_dir():
                # Check if directory contains CSV files
                csv_files = list(item.glob("*.csv"))
                if csv_files:
                    datasets.append(item.name)
                    
        return sorted(datasets)
    
    def get_source_columns(self, dataset_name: str) -> Set[str]:
        """
        Extract all source columns from a dataset.
        
        Args:
            dataset_name: Name of the dataset directory
            
        Returns:
            Set of all column names found in the dataset
        """
        dataset_path = self.data_dir / dataset_name
        all_columns = set()
        
        try:
            # Find all CSV files in the dataset
            csv_files = list(dataset_path.glob("*.csv"))
            
            for csv_file in csv_files:
                try:
                    # Read CSV and extract column names
                    df = pd.read_csv(csv_file)
                    all_columns.update(df.columns.tolist())
                except Exception as e:
                    print(f"⚠️  Warning: Could not read {csv_file}: {e}")
                    
        except Exception as e:
            print(f"❌ Error reading dataset {dataset_name}: {e}")
            
        return all_columns
    
    def get_mapped_columns(self, dataset_name: str) -> Set[str]:
        """
        Extract mapped columns from STTM file.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Set of mapped column names
        """
        # Try different possible STTM file names
        possible_names = [
            # Standard patterns
            f"{dataset_name.lower()}_sttm.json",
            f"{dataset_name.lower()}_sttm_enhanced.json",
            f"{dataset_name.lower()}_sttm_universal.json",
            f"{dataset_name.lower()}_sttm_optimized.json",
            f"{dataset_name.lower()}_sttm_100percent.json",
            f"{dataset_name.lower()}_sttm_scalable.json",
            f"{dataset_name.lower()}_sttm_final.json",
            f"{dataset_name.lower()}_sttm_aggressive.json",
            f"{dataset_name.lower()}_sttm_ultimate.json",
            f"{dataset_name.lower()}_sttm_targeted.json",
            f"{dataset_name.lower()}_sttm_ultra.json",
            f"{dataset_name.lower()}_sttm_comprehensive.json",
            f"{dataset_name.lower()}_sttm_complete.json",
            f"{dataset_name.lower()}_sttm_full.json",
            f"{dataset_name.lower()}_sttm_total.json",
            f"{dataset_name.lower()}_sttm_all.json",
            f"{dataset_name.lower()}_sttm_everything.json",
            f"{dataset_name.lower()}_sttm_perfect.json",
            f"{dataset_name.lower()}_sttm_100.json",
            f"{dataset_name.lower()}_sttm_latest.json",
            f"{dataset_name.lower()}_sttm_v3.json",
            # Variations with underscores instead of hyphens
            f"{dataset_name.lower().replace('-', '_')}_sttm.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_enhanced.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_universal.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_optimized.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_100percent.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_scalable.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_final.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_aggressive.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_ultimate.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_targeted.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_latest.json",
            f"{dataset_name.lower().replace('-', '_')}_sttm_v3.json",
            # Variations with hyphens instead of underscores
            f"{dataset_name.lower().replace('_', '-')}_sttm.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_enhanced.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_universal.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_optimized.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_100percent.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_scalable.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_final.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_aggressive.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_ultimate.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_targeted.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_latest.json",
            f"{dataset_name.lower().replace('_', '-')}_sttm_v3.json",
            # Specific dataset variations
            f"{dataset_name.lower().replace('-sales-data', '_sales')}_sttm.json",
            f"{dataset_name.lower().replace('-sales-data', '_sales')}_sttm_aggressive.json",
            f"{dataset_name.lower().replace('-sales-data', '_sales')}_sttm_latest.json",
            f"{dataset_name.lower().replace('-sales-data', '_sales')}_sttm_v3.json",
            f"{dataset_name.lower().replace('-property-sales-data', '_property')}_sttm.json",
            f"{dataset_name.lower().replace('-property-sales-data', '_property')}_sttm_aggressive.json",
            f"{dataset_name.lower().replace('-property-sales-data', '_property')}_sttm_latest.json",
            f"{dataset_name.lower().replace('-property-sales-data', '_property')}_sttm_v3.json",
            f"{dataset_name.lower().replace('-sales-data', '_sales_data')}_sttm.json",
            f"{dataset_name.lower().replace('-sales-data', '_sales_data')}_sttm_aggressive.json",
            f"{dataset_name.lower().replace('-sales-data', '_sales_data')}_sttm_latest.json",
            f"{dataset_name.lower().replace('-sales-data', '_sales_data')}_sttm_v3.json",
            f"{dataset_name.lower().replace('-property-sales-data', '_property_sales_data')}_sttm.json",
            f"{dataset_name.lower().replace('-property-sales-data', '_property_sales_data')}_sttm_aggressive.json",
            f"{dataset_name.lower().replace('-property-sales-data', '_property_sales_data')}_sttm_latest.json",
            f"{dataset_name.lower().replace('-property-sales-data', '_property_sales_data')}_sttm_v3.json",
            # Remove all special characters
            f"{dataset_name.lower().replace('-', '').replace('_', '')}_sttm.json",
            f"{dataset_name.lower().replace('-', '').replace('_', '')}_sttm_aggressive.json",
            f"{dataset_name.lower().replace('-', '').replace('_', '')}_sttm_latest.json",
            f"{dataset_name.lower().replace('-', '').replace('_', '')}_sttm_v3.json",
            # Pharma specific
            f"pharma2_sttm.json",
            f"pharma2_sttm_aggressive.json",
            f"pharma2_sttm_latest.json",
            f"pharma2_sttm_v3.json",
            f"pharma_sttm.json",
            f"pharma_sttm_aggressive.json",
            f"pharma_sttm_latest.json",
            f"pharma_sttm_v3.json",
            # Amazon specific
            f"amazon_sales_sttm.json",
            f"amazon_sales_sttm_aggressive.json",
            f"amazon_sales_sttm_latest.json",
            f"amazon_sales_sttm_v3.json",
            f"amazon_sales_data_sttm.json",
            f"amazon_sales_data_sttm_aggressive.json",
            f"amazon_sales_data_sttm_latest.json",
            f"amazon_sales_data_sttm_v3.json",
            # NYC specific
            f"nyc_property_sttm.json",
            f"nyc_property_sttm_aggressive.json",
            f"nyc_property_sttm_latest.json",
            f"nyc_property_sttm_v3.json",
            f"nyc_property_sales_data_sttm.json",
            f"nyc_property_sales_data_sttm_aggressive.json",
            f"nyc_property_sales_data_sttm_latest.json",
            f"nyc_property_sales_data_sttm_v3.json"
        ]
        
        for filename in possible_names:
            sttm_path = self.output_dir / filename
            if sttm_path.exists():
                try:
                    with open(sttm_path, 'r') as f:
                        sttm_data = json.load(f)
                    
                    mapped_columns = set()
                    for table in sttm_data.get('tables', []):
                        for column in table.get('columns', []):
                            source_col = column.get('source_column')
                            if source_col:
                                mapped_columns.add(source_col)
                    
                    return mapped_columns
                    
                except Exception as e:
                    print(f"⚠️  Warning: Could not read STTM file {filename}: {e}")
                    continue
        
        print(f"⚠️  Warning: No STTM file found for dataset {dataset_name}")
        return set()
    
    def categorize_columns(self, columns: Set[str], dataset_name: str) -> Dict[str, List[str]]:
        """
        Categorize columns by their business importance.
        
        Args:
            columns: Set of column names
            dataset_name: Name of the dataset for context
            
        Returns:
            Dictionary with categorized columns
        """
        categories = {
            'high_priority': [],
            'medium_priority': [],
            'low_priority': [],
            'unknown': []
        }
        
        # Define priority patterns
        high_priority_patterns = [
            'price', 'cost', 'amount', 'value', 'revenue', 'sales',
            'customer', 'user', 'email', 'phone', 'premium', 'member',
            'rating', 'review', 'brand', 'sku',
            'date', 'time', 'created', 'updated', 'modified'
        ]
        
        medium_priority_patterns = [
            'address', 'city', 'state', 'country', 'zip', 'postal',
            'description', 'content', 'title', 'name', 'link', 'url',
            'image', 'img', 'photo', 'picture', 'unit', 'quantity',
            'discount', 'percentage', 'tax', 'fee', 'charge'
        ]
        
        low_priority_patterns = [
            'unnamed', 'index', 'id_', '_id', 'order', 'number',
            'sequence', 'counter', 'flag', 'status', 'type', 'code',
            'class', 'group', 'level', 'tier', 'rank'
        ]
        
        for column in columns:
            column_lower = column.lower()
            
            # Check high priority patterns
            if any(pattern in column_lower for pattern in high_priority_patterns):
                categories['high_priority'].append(column)
            # Check medium priority patterns
            elif any(pattern in column_lower for pattern in medium_priority_patterns):
                categories['medium_priority'].append(column)
            # Check low priority patterns
            elif any(pattern in column_lower for pattern in low_priority_patterns):
                categories['low_priority'].append(column)
            else:
                categories['unknown'].append(column)
        
        return categories
    
    def analyze_dataset(self, dataset_name: str) -> Dict:
        """
        Perform comprehensive analysis of a single dataset.
        
        Args:
            dataset_name: Name of the dataset to analyze
            
        Returns:
            Dictionary with analysis results
        """
        print(f"🔍 Analyzing dataset: {dataset_name}")
        
        # Get source and mapped columns
        source_columns = self.get_source_columns(dataset_name)
        mapped_columns = self.get_mapped_columns(dataset_name)
        
        # Calculate unmapped columns
        unmapped_columns = source_columns - mapped_columns
        
        # Calculate coverage statistics
        total_columns = len(source_columns)
        mapped_count = len(mapped_columns)
        unmapped_count = len(unmapped_columns)
        coverage_percentage = (mapped_count / total_columns * 100) if total_columns > 0 else 0
        
        # Categorize unmapped columns
        categorized_unmapped = self.categorize_columns(unmapped_columns, dataset_name)
        
        # Analyze housekeeping columns and null handling
        housekeeping_analysis = self.analyze_housekeeping_columns(dataset_name)
        null_handling_analysis = self.analyze_null_handling(dataset_name)
        
        # Create analysis result
        analysis = {
            'dataset_name': dataset_name,
            'total_columns': total_columns,
            'mapped_columns': list(mapped_columns),
            'unmapped_columns': list(unmapped_columns),
            'mapped_count': mapped_count,
            'unmapped_count': unmapped_count,
            'coverage_percentage': round(coverage_percentage, 2),
            'categorized_unmapped': categorized_unmapped,
            'housekeeping_analysis': housekeeping_analysis,
            'null_handling_analysis': null_handling_analysis,
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        return analysis
    
    def analyze_housekeeping_columns(self, dataset_name: str) -> Dict:
        """
        Analyze housekeeping columns in STTM for a dataset.
        
        Args:
            dataset_name: Name of the dataset to analyze
            
        Returns:
            Dictionary with housekeeping analysis results
        """
        sttm_file = self.output_dir / f"{dataset_name}_sttm.json"
        
        if not sttm_file.exists():
            return {
                "error": "STTM file not found",
                "housekeeping_compliance": False
            }
        
        try:
            with open(sttm_file, 'r') as f:
                sttm_data = json.load(f)
            
            required_housekeeping = ['created_timestamp', 'updated_timestamp', 'source_system', 'batch_id', 'is_active', 'version']
            
            analysis = {
                "tables_analyzed": 0,
                "tables_with_all_housekeeping": 0,
                "tables_with_missing_housekeeping": 0,
                "missing_columns_by_table": {},
                "housekeeping_compliance": True
            }
            
            for table in sttm_data.get('tables', []):
                analysis["tables_analyzed"] += 1
                table_columns = {col['target_column']: col for col in table.get('columns', [])}
                
                missing_housekeeping = [col for col in required_housekeeping if col not in table_columns]
                
                if missing_housekeeping:
                    analysis["tables_with_missing_housekeeping"] += 1
                    analysis["missing_columns_by_table"][table['target_table_name']] = missing_housekeeping
                    analysis["housekeeping_compliance"] = False
                else:
                    analysis["tables_with_all_housekeeping"] += 1
            
            return analysis
            
        except Exception as e:
            return {
                "error": f"Failed to analyze housekeeping columns: {str(e)}",
                "housekeeping_compliance": False
            }
    
    def analyze_null_handling(self, dataset_name: str) -> Dict:
        """
        Analyze null handling in STTM for a dataset.
        
        Args:
            dataset_name: Name of the dataset to analyze
            
        Returns:
            Dictionary with null handling analysis results
        """
        sttm_file = self.output_dir / f"{dataset_name}_sttm.json"
        
        if not sttm_file.exists():
            return {
                "error": "STTM file not found",
                "null_handling_compliance": False
            }
        
        try:
            with open(sttm_file, 'r') as f:
                sttm_data = json.load(f)
            
            analysis = {
                "tables_analyzed": 0,
                "tables_with_coalesce": 0,
                "tables_without_coalesce": 0,
                "columns_without_coalesce_by_table": {},
                "null_handling_compliance": True
            }
            
            for table in sttm_data.get('tables', []):
                analysis["tables_analyzed"] += 1
                
                non_housekeeping_columns = [
                    col for col in table.get('columns', [])
                    if col['target_column'] not in ['created_timestamp', 'updated_timestamp', 'source_system', 'batch_id', 'is_active', 'version']
                ]
                
                columns_without_coalesce = [
                    col['target_column'] for col in non_housekeeping_columns
                    if col.get('source_column') and 'coalesce(' not in col.get('transformation_rule', '')
                ]
                
                if columns_without_coalesce:
                    analysis["tables_without_coalesce"] += 1
                    analysis["columns_without_coalesce_by_table"][table['target_table_name']] = columns_without_coalesce
                    analysis["null_handling_compliance"] = False
                else:
                    analysis["tables_with_coalesce"] += 1
            
            return analysis
            
        except Exception as e:
            return {
                "error": f"Failed to analyze null handling: {str(e)}",
                "null_handling_compliance": False
            }
    
    def analyze_all_datasets(self) -> Dict:
        """
        Analyze all discovered datasets.
        
        Returns:
            Dictionary with comprehensive analysis results
        """
        print("🚀 Starting comprehensive unmapped columns analysis...")
        print(f"📁 Data directory: {self.data_dir}")
        print(f"📁 Output directory: {self.output_dir}")
        print()
        
        # Discover datasets
        datasets = self.discover_datasets()
        if not datasets:
            print("❌ No datasets found!")
            return {}
        
        print(f"📊 Found {len(datasets)} datasets: {', '.join(datasets)}")
        print()
        
        # Analyze each dataset
        all_results = {}
        total_stats = {
            'total_datasets': len(datasets),
            'total_columns': 0,
            'total_mapped': 0,
            'total_unmapped': 0,
            'perfect_coverage_datasets': 0,
            'moderate_coverage_datasets': 0,
            'low_coverage_datasets': 0
        }
        
        for dataset in datasets:
            analysis = self.analyze_dataset(dataset)
            all_results[dataset] = analysis
            
            # Update total statistics
            total_stats['total_columns'] += analysis['total_columns']
            total_stats['total_mapped'] += analysis['mapped_count']
            total_stats['total_unmapped'] += analysis['unmapped_count']
            
            # Categorize by coverage
            if analysis['coverage_percentage'] == 100:
                total_stats['perfect_coverage_datasets'] += 1
            elif analysis['coverage_percentage'] >= 60:
                total_stats['moderate_coverage_datasets'] += 1
            else:
                total_stats['low_coverage_datasets'] += 1
        
        # Calculate overall coverage
        overall_coverage = (total_stats['total_mapped'] / total_stats['total_columns'] * 100) if total_stats['total_columns'] > 0 else 0
        
        # Compile comprehensive results
        comprehensive_results = {
            'analysis_metadata': {
                'timestamp': datetime.now().isoformat(),
                'data_directory': str(self.data_dir),
                'output_directory': str(self.output_dir),
                'total_datasets_analyzed': len(datasets)
            },
            'overall_statistics': {
                'total_datasets': total_stats['total_datasets'],
                'total_columns': total_stats['total_columns'],
                'total_mapped': total_stats['total_mapped'],
                'total_unmapped': total_stats['total_unmapped'],
                'overall_coverage_percentage': round(overall_coverage, 2),
                'perfect_coverage_datasets': total_stats['perfect_coverage_datasets'],
                'moderate_coverage_datasets': total_stats['moderate_coverage_datasets'],
                'low_coverage_datasets': total_stats['low_coverage_datasets']
            },
            'dataset_analyses': all_results
        }
        
        self.results = comprehensive_results
        return comprehensive_results
    
    def generate_summary_report(self) -> str:
        """
        Generate a human-readable summary report.
        
        Returns:
            Formatted summary report string
        """
        if not self.results:
            return "❌ No analysis results available. Run analyze_all_datasets() first."
        
        stats = self.results['overall_statistics']
        metadata = self.results['analysis_metadata']
        
        report = f"""
{'='*80}
                    COMPREHENSIVE UNMAPPED COLUMNS ANALYSIS REPORT
{'='*80}

📊 EXECUTIVE SUMMARY
{'='*80}
Analysis Timestamp: {metadata['timestamp']}
Data Directory: {metadata['data_directory']}
Output Directory: {metadata['output_directory']}
Total Datasets Analyzed: {metadata['total_datasets_analyzed']}

📈 OVERALL STATISTICS
{'='*80}
Total Columns: {stats['total_columns']}
Total Mapped: {stats['total_mapped']}
Total Unmapped: {stats['total_unmapped']}
Overall Coverage: {stats['overall_coverage_percentage']}%

📋 COVERAGE BREAKDOWN
{'='*80}
Perfect Coverage (100%): {stats['perfect_coverage_datasets']} datasets
Moderate Coverage (60-99%): {stats['moderate_coverage_datasets']} datasets
Low Coverage (<60%): {stats['low_coverage_datasets']} datasets

📊 DETAILED DATASET ANALYSIS
{'='*80}
"""
        
        # Add dataset-specific details
        for dataset_name, analysis in self.results['dataset_analyses'].items():
            coverage_status = "✅" if analysis['coverage_percentage'] == 100 else "⚠️" if analysis['coverage_percentage'] >= 60 else "❌"
            
            report += f"""
{coverage_status} {dataset_name.upper()} DATASET
{'-'*60}
Coverage: {analysis['mapped_count']}/{analysis['total_columns']} columns ({analysis['coverage_percentage']}%)
Unmapped Columns: {analysis['unmapped_count']}

High Priority Unmapped: {len(analysis['categorized_unmapped']['high_priority'])}
Medium Priority Unmapped: {len(analysis['categorized_unmapped']['medium_priority'])}
Low Priority Unmapped: {len(analysis['categorized_unmapped']['low_priority'])}

Unmapped Columns by Priority:
"""
            
            for priority, columns in analysis['categorized_unmapped'].items():
                if columns:
                    report += f"  {priority.replace('_', ' ').title()}: {', '.join(columns[:5])}"
                    if len(columns) > 5:
                        report += f" ... and {len(columns) - 5} more"
                    report += "\n"
            
            report += "\n"
        
        report += f"""
{'='*80}
                                ANALYSIS COMPLETE
{'='*80}
"""
        
        return report
    
    def export_results(self, format: str = 'json', output_path: Optional[str] = None) -> str:
        """
        Export analysis results to various formats.
        
        Args:
            format: Export format ('json', 'csv', 'html')
            output_path: Custom output path (optional)
            
        Returns:
            Path to exported file
        """
        if not self.results:
            print("❌ No results to export. Run analyze_all_datasets() first.")
            return ""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_path is None:
            output_path = f"unmapped_columns_analysis_{timestamp}"
        
        if format.lower() == 'json':
            file_path = f"{output_path}.json"
            with open(file_path, 'w') as f:
                json.dump(self.results, f, indent=2)
                
        elif format.lower() == 'csv':
            file_path = f"{output_path}.csv"
            # Create CSV with dataset summary
            rows = []
            for dataset_name, analysis in self.results['dataset_analyses'].items():
                row = {
                    'dataset_name': dataset_name,
                    'total_columns': analysis['total_columns'],
                    'mapped_columns': analysis['mapped_count'],
                    'unmapped_columns': analysis['unmapped_count'],
                    'coverage_percentage': analysis['coverage_percentage'],
                    'high_priority_unmapped': len(analysis['categorized_unmapped']['high_priority']),
                    'medium_priority_unmapped': len(analysis['categorized_unmapped']['medium_priority']),
                    'low_priority_unmapped': len(analysis['categorized_unmapped']['low_priority'])
                }
                rows.append(row)
            
            df = pd.DataFrame(rows)
            df.to_csv(file_path, index=False)
            
        elif format.lower() == 'html':
            file_path = f"{output_path}.html"
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Unmapped Columns Analysis Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .dataset {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
        .perfect {{ border-left: 5px solid #4CAF50; }}
        .moderate {{ border-left: 5px solid #FF9800; }}
        .low {{ border-left: 5px solid #f44336; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin: 10px 0; }}
        .stat {{ background-color: #f9f9f9; padding: 10px; border-radius: 3px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Unmapped Columns Analysis Report</h1>
        <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Overall Coverage: {self.results['overall_statistics']['overall_coverage_percentage']}%</p>
    </div>
"""
            
            for dataset_name, analysis in self.results['dataset_analyses'].items():
                coverage_class = 'perfect' if analysis['coverage_percentage'] == 100 else 'moderate' if analysis['coverage_percentage'] >= 60 else 'low'
                
                html_content += f"""
    <div class="dataset {coverage_class}">
        <h2>{dataset_name.upper()}</h2>
        <div class="stats">
            <div class="stat">
                <strong>Coverage:</strong> {analysis['coverage_percentage']}%
            </div>
            <div class="stat">
                <strong>Total Columns:</strong> {analysis['total_columns']}
            </div>
            <div class="stat">
                <strong>Mapped:</strong> {analysis['mapped_count']}
            </div>
            <div class="stat">
                <strong>Unmapped:</strong> {analysis['unmapped_count']}
            </div>
        </div>
        <h3>Unmapped Columns by Priority:</h3>
"""
                
                for priority, columns in analysis['categorized_unmapped'].items():
                    if columns:
                        html_content += f"<p><strong>{priority.replace('_', ' ').title()}:</strong> {', '.join(columns)}</p>"
                
                html_content += "</div>"
            
            html_content += """
</body>
</html>
"""
            
            with open(file_path, 'w') as f:
                f.write(html_content)
        
        else:
            print(f"❌ Unsupported format: {format}")
            return ""
        
        print(f"✅ Results exported to: {file_path}")
        return file_path


# Test functions
def test_dataset_discovery():
    """Test that datasets can be discovered automatically."""
    analyzer = UnmappedColumnsAnalyzer()
    datasets = analyzer.discover_datasets()
    
    # Assert that we found some datasets
    assert len(datasets) > 0, "No datasets found in data directory"
    
    # Assert that all discovered items are directories with CSV files
    for dataset in datasets:
        dataset_path = analyzer.data_dir / dataset
        assert dataset_path.is_dir(), f"Dataset {dataset} is not a directory"
        
        csv_files = list(dataset_path.glob("*.csv"))
        assert len(csv_files) > 0, f"Dataset {dataset} contains no CSV files"
    
    print(f"✅ Discovered {len(datasets)} datasets: {', '.join(datasets)}")


def test_source_columns_extraction():
    """Test that source columns can be extracted from datasets."""
    analyzer = UnmappedColumnsAnalyzer()
    datasets = analyzer.discover_datasets()
    
    for dataset in datasets[:3]:  # Test first 3 datasets
        columns = analyzer.get_source_columns(dataset)
        assert len(columns) > 0, f"No columns found in dataset {dataset}"
        print(f"✅ Dataset {dataset}: {len(columns)} source columns")


def test_sttm_file_detection():
    """Test that STTM files can be detected and read."""
    analyzer = UnmappedColumnsAnalyzer()
    datasets = analyzer.discover_datasets()
    
    sttm_files_found = 0
    for dataset in datasets:
        mapped_columns = analyzer.get_mapped_columns(dataset)
        if mapped_columns:
            sttm_files_found += 1
            print(f"✅ STTM file found for {dataset}: {len(mapped_columns)} mapped columns")
    
    assert sttm_files_found > 0, "No STTM files found in output directory"


def test_comprehensive_analysis():
    """Test comprehensive analysis of all datasets."""
    analyzer = UnmappedColumnsAnalyzer()
    results = analyzer.analyze_all_datasets()
    
    # Assert that analysis was successful
    assert results, "Analysis failed to produce results"
    
    # Assert that we have metadata
    assert 'analysis_metadata' in results, "Missing analysis metadata"
    assert 'overall_statistics' in results, "Missing overall statistics"
    assert 'dataset_analyses' in results, "Missing dataset analyses"
    
    # Assert that we analyzed some datasets
    stats = results['overall_statistics']
    assert stats['total_datasets'] > 0, "No datasets were analyzed"
    assert stats['total_columns'] > 0, "No columns were found"
    
    # Print summary report
    summary = analyzer.generate_summary_report()
    print(summary)
    
    # Assert minimum coverage expectations
    overall_coverage = stats['overall_coverage_percentage']
    assert overall_coverage >= 50, f"Overall coverage too low: {overall_coverage}%"
    
    print(f"✅ Comprehensive analysis completed successfully!")
    print(f"📊 Overall coverage: {overall_coverage}%")
    print(f"📁 Datasets analyzed: {stats['total_datasets']}")
    print(f"📋 Total columns: {stats['total_columns']}")


def test_column_categorization():
    """Test that columns are properly categorized by priority."""
    analyzer = UnmappedColumnsAnalyzer()
    
    # Test with sample columns
    test_columns = {
        'price', 'customer_email', 'shipping_address', 
        'order_number', 'product_description', 'unknown_field'
    }
    
    categorized = analyzer.categorize_columns(test_columns, 'test_dataset')
    
    # Assert that categorization worked
    assert 'high_priority' in categorized, "Missing high_priority category"
    assert 'medium_priority' in categorized, "Missing medium_priority category"
    assert 'low_priority' in categorized, "Missing low_priority category"
    assert 'unknown' in categorized, "Missing unknown category"
    
    # Assert that high priority columns were identified
    assert 'price' in categorized['high_priority'], "Price should be high priority"
    assert 'customer_email' in categorized['high_priority'], "Customer email should be high priority"
    
    # Assert that medium priority columns were identified
    assert 'shipping_address' in categorized['medium_priority'], "Shipping address should be medium priority"
    assert 'product_description' in categorized['medium_priority'], "Product description should be medium priority"
    
    # Assert that low priority columns were identified
    assert 'order_number' in categorized['low_priority'], "Order number should be low priority"
    
    print("✅ Column categorization working correctly")


def test_export_functionality():
    """Test export functionality."""
    analyzer = UnmappedColumnsAnalyzer()
    results = analyzer.analyze_all_datasets()
    
    if results:
        # Test JSON export
        json_file = analyzer.export_results('json', 'test_analysis')
        assert Path(json_file).exists(), "JSON export failed"
        
        # Test CSV export
        csv_file = analyzer.export_results('csv', 'test_analysis')
        assert Path(csv_file).exists(), "CSV export failed"
        
        # Clean up test files
        for file_path in [json_file, csv_file]:
            if Path(file_path).exists():
                Path(file_path).unlink()
        
        print("✅ Export functionality working correctly")


def test_coverage_quality_assertions():
    """Test quality assertions for coverage."""
    analyzer = UnmappedColumnsAnalyzer()
    results = analyzer.analyze_all_datasets()
    
    if not results:
        pytest.skip("No analysis results available")
    
    stats = results['overall_statistics']
    
    # Quality assertions
    assert stats['overall_coverage_percentage'] >= 50, f"Overall coverage below 50%: {stats['overall_coverage_percentage']}%"
    assert stats['perfect_coverage_datasets'] >= 1, "No datasets with perfect coverage found"
    assert stats['total_unmapped'] < stats['total_columns'], "All columns should not be unmapped"
    
    # Check for high priority unmapped columns
    high_priority_unmapped_count = 0
    for dataset_name, analysis in results['dataset_analyses'].items():
        high_priority_unmapped_count += len(analysis['categorized_unmapped']['high_priority'])
    
    print(f"📊 Quality check results:")
    print(f"   Overall coverage: {stats['overall_coverage_percentage']}%")
    print(f"   Perfect coverage datasets: {stats['perfect_coverage_datasets']}")
    print(f"   High priority unmapped columns: {high_priority_unmapped_count}")
    
    # Warning if too many high priority columns are unmapped
    if high_priority_unmapped_count > 10:
        print(f"⚠️  Warning: {high_priority_unmapped_count} high priority columns are unmapped")
    
    print("✅ Quality assertions passed")


# Main test runner
if __name__ == "__main__":
    """Run the comprehensive analysis as a standalone script."""
    print("🚀 Running Comprehensive Unmapped Columns Analysis Test")
    print("=" * 80)
    
    # Run all tests
    test_functions = [
        test_dataset_discovery,
        test_source_columns_extraction,
        test_sttm_file_detection,
        test_column_categorization,
        test_comprehensive_analysis,
        test_export_functionality,
        test_coverage_quality_assertions
    ]
    
    passed = 0
    failed = 0
    
    for test_func in test_functions:
        try:
            print(f"\n🧪 Running {test_func.__name__}...")
            test_func()
            passed += 1
            print(f"✅ {test_func.__name__} passed")
        except Exception as e:
            failed += 1
            print(f"❌ {test_func.__name__} failed: {e}")
    
    print("\n" + "=" * 80)
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed!")
        sys.exit(0)
    else:
        print("⚠️  Some tests failed!")
        sys.exit(1) 