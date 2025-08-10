#!/usr/bin/env python3
"""
Comprehensive Accuracy Test for Analyst Agent.

This test runs all accuracy validation tests and provides a detailed report
on the Analyst Agent's performance across multiple dimensions.
"""

import json
import sys
import os
import time
from pathlib import Path
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from nuvyn_bldr.agents.analyst.agent import AnalystAgent
from nuvyn_bldr.schemas.sttm import STTM


def generate_comprehensive_report():
    """Generate a comprehensive accuracy report for the Analyst Agent."""
    print("🚀 COMPREHENSIVE ANALYST AGENT ACCURACY REPORT")
    print("=" * 80)
    print(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Initialize agent
    agent = AnalystAgent()
    
    # Test datasets
    datasets = [
        ("E-commerce", "data/ecommerce"),
        ("Pharmaceutical", "data/Pharma"),
        ("Stock Trading", "data/stock")
    ]
    
    results = {}
    
    for dataset_name, data_path in datasets:
        print(f"\n{'='*20} TESTING {dataset_name.upper()} {'='*20}")
        
        try:
            # Generate STTM
            data_dir = Path(__file__).parent.parent / data_path
            file_paths = [str(f) for f in data_dir.glob("*.csv")]
            
            if not file_paths:
                print(f"⚠️  No CSV files found in {data_path}")
                continue
            
            start_time = time.time()
            sttm = agent.generate_sttm(file_paths)
            end_time = time.time()
            
            generation_time = end_time - start_time
            
            # Analyze results
            analysis = analyze_sttm_comprehensive(sttm, dataset_name, generation_time)
            results[dataset_name] = analysis
            
            print(f"✅ {dataset_name} analysis completed in {generation_time:.2f}s")
            
        except Exception as e:
            print(f"❌ {dataset_name} failed: {str(e)}")
            results[dataset_name] = {"error": str(e)}
    
    # Generate summary report
    print(f"\n{'='*80}")
    print("📊 COMPREHENSIVE ACCURACY SUMMARY")
    print("=" * 80)
    
    summary = generate_summary_report(results)
    
    # Save detailed report
    save_detailed_report(results, summary)
    
    return summary


def validate_housekeeping_columns_comprehensive(table):
    """Validate housekeeping columns for comprehensive testing."""
    housekeeping_columns = {
        'created_timestamp': {'type': 'TimestampType()', 'rule': 'current_timestamp()'},
        'updated_timestamp': {'type': 'TimestampType()', 'rule': 'current_timestamp()'},
        'source_system': {'type': 'StringType()', 'rule': 'literal'},
        'batch_id': {'type': 'StringType()', 'rule': 'literal'},
        'is_active': {'type': 'BooleanType()', 'rule': 'literal(true)'},
        'version': {'type': 'IntegerType()', 'rule': 'literal(1)'}
    }
    
    table_columns = {col.target_column: col for col in table.columns}
    
    validation_result = {
        'present': [],
        'missing': [],
        'type_errors': [],
        'rule_errors': []
    }
    
    for col_name, expected in housekeeping_columns.items():
        if col_name in table_columns:
            validation_result['present'].append(col_name)
            col = table_columns[col_name]
            
            if expected['type'] not in col.target_type:
                validation_result['type_errors'].append(f"{col_name}: {col.target_type}")
            
            if expected['rule'] not in col.transformation_rule:
                validation_result['rule_errors'].append(f"{col_name}: {col.transformation_rule}")
        else:
            validation_result['missing'].append(col_name)
    
    return validation_result


def validate_null_handling_comprehensive(table):
    """Validate null handling for comprehensive testing."""
    non_housekeeping_columns = [
        col for col in table.columns 
        if col.target_column not in ['created_timestamp', 'updated_timestamp', 'source_system', 'batch_id', 'is_active', 'version']
    ]
    
    validation_result = {
        'with_coalesce': [],
        'without_coalesce': [],
        'total_columns': len(non_housekeeping_columns)
    }
    
    for col in non_housekeeping_columns:
        if col.source_column:
            if 'coalesce(' in col.transformation_rule:
                validation_result['with_coalesce'].append(col.target_column)
            else:
                validation_result['without_coalesce'].append(col.target_column)
    
    return validation_result


def analyze_sttm_comprehensive(sttm: STTM, dataset_name: str, generation_time: float):
    """Comprehensive analysis of an STTM document."""
    
    analysis = {
        "dataset": dataset_name,
        "generation_time": generation_time,
        "timestamp": datetime.now().isoformat(),
        "metrics": {}
    }
    
    # Basic structure metrics
    fact_tables = [t for t in sttm.tables if t.table_type == "fact"]
    dim_tables = [t for t in sttm.tables if t.table_type == "dimension"]
    
    analysis["metrics"]["structure"] = {
        "total_tables": len(sttm.tables),
        "fact_tables": len(fact_tables),
        "dimension_tables": len(dim_tables),
        "has_primary_keys": all(len(t.primary_key) > 0 for t in sttm.tables),
        "has_foreign_keys": any(t.foreign_keys for t in fact_tables)
    }
    
    # Schema quality metrics
    analysis["metrics"]["schema_quality"] = {
        "star_schema_compliance": len(fact_tables) >= 1 and len(dim_tables) >= 1,
        "proper_naming": all(t.target_table_name.startswith(("fact_", "dim_")) for t in sttm.tables),
        "consistent_primary_keys": all(len(t.primary_key) == 1 for t in sttm.tables)
    }
    
    # Business logic metrics
    business_measures = []
    descriptive_attributes = []
    time_columns = []
    
    for table in sttm.tables:
        for col in table.columns:
            col_name = col.source_column.lower()
            
            # Business measures
            if any(measure in col_name for measure in ["amount", "price", "quantity", "total", "count", "sum", "value"]):
                business_measures.append(col.source_column)
            
            # Descriptive attributes
            if any(desc in col_name for desc in ["name", "description", "address", "city", "state", "category", "brand"]):
                descriptive_attributes.append(col.source_column)
            
            # Time columns
            if any(time_word in col_name for time_word in ["date", "time", "hour", "day", "week", "month"]):
                time_columns.append(col.source_column)
    
    analysis["metrics"]["business_logic"] = {
        "business_measures_count": len(business_measures),
        "descriptive_attributes_count": len(descriptive_attributes),
        "time_columns_count": len(time_columns),
        "has_business_measures": len(business_measures) > 0,
        "has_descriptive_attributes": len(descriptive_attributes) > 0,
        "has_time_analysis": len(time_columns) > 0
    }
    
    # Data type quality metrics
    type_issues = []
    for table in sttm.tables:
        for col in table.columns:
            col_name = col.source_column.lower()
            target_type = col.target_type
            
            if "id" in col_name and "StringType" in target_type and "name" not in col_name:
                type_issues.append(f"ID as string: {col.source_column}")
            elif "price" in col_name and "IntegerType" in target_type:
                type_issues.append(f"Price as integer: {col.source_column}")
            elif "date" in col_name and "StringType" in target_type:
                type_issues.append(f"Date as string: {col.source_column}")
    
    analysis["metrics"]["data_quality"] = {
        "type_issues_count": len(type_issues),
        "type_issues": type_issues,
        "type_accuracy": max(0, 100 - len(type_issues) * 10)
    }
    
    # Housekeeping columns metrics
    housekeeping_validation = {
        "tables_with_all_housekeeping": 0,
        "tables_with_missing_housekeeping": 0,
        "total_housekeeping_columns": 0,
        "missing_housekeeping_columns": [],
        "type_errors": [],
        "rule_errors": []
    }
    
    for table in sttm.tables:
        validation = validate_housekeeping_columns_comprehensive(table)
        if len(validation['missing']) == 0 and len(validation['type_errors']) == 0 and len(validation['rule_errors']) == 0:
            housekeeping_validation["tables_with_all_housekeeping"] += 1
        else:
            housekeeping_validation["tables_with_missing_housekeeping"] += 1
        
        housekeeping_validation["total_housekeeping_columns"] += len(validation['present'])
        housekeeping_validation["missing_housekeeping_columns"].extend(validation['missing'])
        housekeeping_validation["type_errors"].extend(validation['type_errors'])
        housekeeping_validation["rule_errors"].extend(validation['rule_errors'])
    
    analysis["metrics"]["housekeeping"] = housekeeping_validation
    
    # Null handling metrics
    null_handling_validation = {
        "tables_with_coalesce": 0,
        "tables_without_coalesce": 0,
        "total_columns_with_coalesce": 0,
        "total_columns_without_coalesce": 0,
        "columns_without_coalesce": []
    }
    
    for table in sttm.tables:
        validation = validate_null_handling_comprehensive(table)
        if len(validation['without_coalesce']) == 0:
            null_handling_validation["tables_with_coalesce"] += 1
        else:
            null_handling_validation["tables_without_coalesce"] += 1
        
        null_handling_validation["total_columns_with_coalesce"] += len(validation['with_coalesce'])
        null_handling_validation["total_columns_without_coalesce"] += len(validation['without_coalesce'])
        null_handling_validation["columns_without_coalesce"].extend(validation['without_coalesce'])
    
    analysis["metrics"]["null_handling"] = null_handling_validation
    
    # Calculate overall score
    score = calculate_overall_score(analysis["metrics"])
    analysis["metrics"]["overall_score"] = score
    
    return analysis


def calculate_overall_score(metrics):
    """Calculate overall accuracy score."""
    score = 0
    max_score = 100
    
    # Structure score (25 points)
    if metrics["structure"]["has_primary_keys"]:
        score += 15
    if metrics["structure"]["has_foreign_keys"]:
        score += 10
    
    # Schema quality score (25 points)
    if metrics["schema_quality"]["star_schema_compliance"]:
        score += 15
    if metrics["schema_quality"]["proper_naming"]:
        score += 10
    
    # Business logic score (20 points)
    if metrics["business_logic"]["has_business_measures"]:
        score += 10
    if metrics["business_logic"]["has_descriptive_attributes"]:
        score += 10
    
    # Housekeeping columns score (15 points)
    if "housekeeping" in metrics:
        housekeeping = metrics["housekeeping"]
        if housekeeping["tables_with_all_housekeeping"] > 0:
            score += 15
        elif housekeeping["tables_with_missing_housekeeping"] > 0:
            score += 5  # Partial credit
    
    # Null handling score (15 points)
    if "null_handling" in metrics:
        null_handling = metrics["null_handling"]
        if null_handling["tables_with_coalesce"] > 0:
            score += 15
        elif null_handling["tables_without_coalesce"] > 0:
            score += 5  # Partial credit
    
    # Data quality score (20 points)
    score += metrics["data_quality"]["type_accuracy"] * 0.2
    
    return min(score, max_score)


def generate_summary_report(results):
    """Generate a summary report from all results."""
    
    summary = {
        "total_datasets": len(results),
        "successful_datasets": len([r for r in results.values() if "error" not in r]),
        "average_generation_time": 0,
        "average_score": 0,
        "dataset_scores": {},
        "overall_grade": "F"
    }
    
    successful_results = [r for r in results.values() if "error" not in r]
    
    if successful_results:
        # Calculate averages
        generation_times = [r["generation_time"] for r in successful_results]
        scores = [r["metrics"]["overall_score"] for r in successful_results]
        
        summary["average_generation_time"] = sum(generation_times) / len(generation_times)
        summary["average_score"] = sum(scores) / len(scores)
        
        # Individual dataset scores
        for dataset_name, result in results.items():
            if "error" not in result:
                summary["dataset_scores"][dataset_name] = {
                    "score": result["metrics"]["overall_score"],
                    "generation_time": result["generation_time"],
                    "structure_quality": result["metrics"]["structure"],
                    "business_logic_quality": result["metrics"]["business_logic"]
                }
        
        # Overall grade
        avg_score = summary["average_score"]
        if avg_score >= 90:
            summary["overall_grade"] = "A"
        elif avg_score >= 80:
            summary["overall_grade"] = "B"
        elif avg_score >= 70:
            summary["overall_grade"] = "C"
        elif avg_score >= 60:
            summary["overall_grade"] = "D"
        else:
            summary["overall_grade"] = "F"
    
    # Print summary
    print(f"📈 Overall Performance: {summary['overall_grade']} ({summary['average_score']:.1f}/100)")
    print(f"⏱️  Average Generation Time: {summary['average_generation_time']:.2f} seconds")
    print(f"✅ Successful Datasets: {summary['successful_datasets']}/{summary['total_datasets']}")
    
    print("\n📊 Dataset Performance:")
    for dataset_name, dataset_result in summary["dataset_scores"].items():
        score = dataset_result["score"]
        time = dataset_result["generation_time"]
        grade = "A" if score >= 90 else "B" if score >= 80 else "C" if score >= 70 else "D" if score >= 60 else "F"
        print(f"  {grade} {dataset_name}: {score:.1f}/100 ({time:.2f}s)")
    
    return summary


def save_detailed_report(results, summary):
    """Save detailed report to file."""
    report = {
        "summary": summary,
        "detailed_results": results,
        "generated_at": datetime.now().isoformat()
    }
    
    output_dir = Path(__file__).parent.parent / "output"
    output_dir.mkdir(exist_ok=True)
    
    report_file = output_dir / "analyst_agent_accuracy_report.json"
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n📄 Detailed report saved to: {report_file}")


def print_recommendations(summary):
    """Print improvement recommendations based on results."""
    print(f"\n{'='*80}")
    print("💡 IMPROVEMENT RECOMMENDATIONS")
    print("=" * 80)
    
    if summary["average_score"] >= 90:
        print("🎉 Excellent performance! The Analyst Agent is working very well.")
        print("   Consider:")
        print("   - Adding more complex business logic validation")
        print("   - Implementing performance optimizations")
        print("   - Adding support for more data formats")
    
    elif summary["average_score"] >= 80:
        print("👍 Good performance with room for improvement.")
        print("   Consider:")
        print("   - Reviewing data type mappings")
        print("   - Improving naming conventions")
        print("   - Adding more business logic validation")
    
    elif summary["average_score"] >= 70:
        print("⚠️  Acceptable performance but needs improvement.")
        print("   Focus on:")
        print("   - Fixing data type issues")
        print("   - Improving star schema design")
        print("   - Enhancing business logic detection")
    
    else:
        print("❌ Performance needs significant improvement.")
        print("   Priority fixes:")
        print("   - Fix critical data type issues")
        print("   - Improve basic schema structure")
        print("   - Enhance prompt engineering")


if __name__ == "__main__":
    summary = generate_comprehensive_report()
    print_recommendations(summary) 