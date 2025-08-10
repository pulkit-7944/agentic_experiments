#!/usr/bin/env python3
"""
Business Logic Accuracy Test for Analyst Agent.

This test validates that the Analyst Agent generates STTM documents that
follow proper business logic and data warehousing best practices.
"""

import json
import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from nuvyn_bldr.agents.analyst.agent import AnalystAgent
from nuvyn_bldr.schemas.sttm import STTM


def analyze_sttm_business_logic(sttm: STTM, dataset_name: str):
    """Analyze the business logic of a generated STTM."""
    print(f"🔍 Analyzing {dataset_name} STTM Business Logic...")
    
    # 1. Validate Star Schema Structure
    fact_tables = [t for t in sttm.tables if t.table_type == "fact"]
    dim_tables = [t for t in sttm.tables if t.table_type == "dimension"]
    
    print(f"  📊 Schema Structure: {len(fact_tables)} fact table(s), {len(dim_tables)} dimension table(s)")
    
    # 2. Validate Fact Table Business Logic
    for fact_table in fact_tables:
        print(f"  🎯 Fact Table: {fact_table.target_table_name}")
        
        # Check for business measures
        measure_columns = []
        for col in fact_table.columns:
            col_name = col.source_column.lower()
            if any(measure in col_name for measure in ["amount", "price", "quantity", "total", "count", "sum", "value"]):
                measure_columns.append(col.source_column)
        
        print(f"    📈 Business Measures: {len(measure_columns)} found - {measure_columns}")
        
        # Check for foreign keys to dimensions
        if fact_table.foreign_keys:
            print(f"    🔗 Foreign Keys: {list(fact_table.foreign_keys.keys())}")
        else:
            print("    ⚠️  No foreign keys found")
    
    # 3. Validate Dimension Table Business Logic
    for dim_table in dim_tables:
        print(f"  📋 Dimension Table: {dim_table.target_table_name}")
        
        # Check for descriptive attributes
        descriptive_columns = []
        for col in dim_table.columns:
            col_name = col.source_column.lower()
            if any(desc in col_name for desc in ["name", "description", "address", "city", "state", "category", "brand"]):
                descriptive_columns.append(col.source_column)
        
        print(f"    📝 Descriptive Attributes: {len(descriptive_columns)} found - {descriptive_columns[:3]}...")
        
        # Check primary key
        if dim_table.primary_key:
            print(f"    🔑 Primary Key: {dim_table.primary_key}")
    
    # 4. Validate Data Type Appropriateness
    print("  🔧 Data Type Analysis:")
    type_issues = []
    
    for table in sttm.tables:
        for col in table.columns:
            col_name = col.source_column.lower()
            target_type = col.target_type
            
            # Check for obvious type mismatches
            if "id" in col_name and "StringType" in target_type and "name" not in col_name:
                type_issues.append(f"ID column '{col.source_column}' mapped to StringType")
            elif "price" in col_name and "IntegerType" in target_type:
                type_issues.append(f"Price column '{col.source_column}' mapped to IntegerType (should be DoubleType)")
            elif "date" in col_name and "StringType" in target_type:
                type_issues.append(f"Date column '{col.source_column}' mapped to StringType (should be DateType/TimestampType)")
    
    if type_issues:
        print(f"    ⚠️  Found {len(type_issues)} potential type issues:")
        for issue in type_issues[:3]:  # Show first 3 issues
            print(f"      - {issue}")
    else:
        print("    ✅ No obvious type issues found")
    
    # 5. Validate Housekeeping Columns
    print("  🏠 Housekeeping Columns Analysis:")
    housekeeping_issues = []
    
    for table in sttm.tables:
        table_columns = {col.target_column: col for col in table.columns}
        required_housekeeping = ['created_timestamp', 'updated_timestamp', 'source_system', 'batch_id', 'is_active', 'version']
        
        missing_housekeeping = [col for col in required_housekeeping if col not in table_columns]
        if missing_housekeeping:
            housekeeping_issues.append(f"Table '{table.target_table_name}' missing: {missing_housekeeping}")
    
    if housekeeping_issues:
        print(f"    ⚠️  Found {len(housekeeping_issues)} housekeeping issues:")
        for issue in housekeeping_issues[:3]:
            print(f"      - {issue}")
    else:
        print("    ✅ All tables have required housekeeping columns")
    
    # 6. Validate Null Handling
    print("  🛡️  Null Handling Analysis:")
    null_handling_issues = []
    
    for table in sttm.tables:
        non_housekeeping_columns = [
            col for col in table.columns 
            if col.target_column not in ['created_timestamp', 'updated_timestamp', 'source_system', 'batch_id', 'is_active', 'version']
        ]
        
        columns_without_coalesce = [
            col.target_column for col in non_housekeeping_columns 
            if col.source_column and 'coalesce(' not in col.transformation_rule
        ]
        
        if columns_without_coalesce:
            null_handling_issues.append(f"Table '{table.target_table_name}' columns without coalesce(): {columns_without_coalesce}")
    
    if null_handling_issues:
        print(f"    ⚠️  Found {len(null_handling_issues)} null handling issues:")
        for issue in null_handling_issues[:3]:
            print(f"      - {issue}")
    else:
        print("    ✅ All columns use coalesce() for null handling")
    
    # 7. Calculate Business Logic Score
    score = 0
    max_score = 100
    
    # Fact table score (25 points)
    if fact_tables:
        score += 25
        if any(len(t.foreign_keys or {}) > 0 for t in fact_tables):
            score += 10
        if any(len([c for c in t.columns if any(m in c.source_column.lower() for m in ["amount", "price", "quantity", "total"])]) > 0 for t in fact_tables):
            score += 10
    
    # Dimension table score (30 points)
    if dim_tables:
        score += 30
        if all(len(t.primary_key) > 0 for t in dim_tables):
            score += 10
        if any(len([c for c in t.columns if any(d in c.source_column.lower() for d in ["name", "description", "address"])]) > 0 for t in dim_tables):
            score += 10
    
    # Type appropriateness score (15 points)
    if not type_issues:
        score += 15
    
    # Housekeeping columns score (10 points)
    if not housekeeping_issues:
        score += 10
    
    # Null handling score (10 points)
    if not null_handling_issues:
        score += 10
    
    print(f"  🎯 Business Logic Score: {score}/{max_score} ({score/max_score*100:.1f}%)")
    
    return score, max_score


def test_ecommerce_business_logic():
    """Test business logic for e-commerce STTM."""
    print("=" * 50)
    print("🛒 E-COMMERCE BUSINESS LOGIC TEST")
    print("=" * 50)
    
    # Generate STTM
    agent = AnalystAgent()
    data_dir = Path(__file__).parent.parent / "data" / "ecommerce"
    file_paths = [str(f) for f in data_dir.glob("*.csv")]
    sttm = agent.generate_sttm(file_paths)
    
    # Analyze business logic
    score, max_score = analyze_sttm_business_logic(sttm, "E-commerce")
    
    # Specific e-commerce validations
    fact_tables = [t for t in sttm.tables if t.table_type == "fact"]
    dim_tables = [t for t in sttm.tables if t.table_type == "dimension"]
    
    # Validate e-commerce specific requirements
    assert len(fact_tables) >= 1, "E-commerce should have at least one fact table (sales)"
    assert len(dim_tables) >= 3, "E-commerce should have customer, product, and store dimensions"
    
    # Check for expected dimensions
    dim_names = [t.target_table_name.lower() for t in dim_tables]
    expected_dims = ["customer", "product", "store"]
    found_dims = [dim for dim in expected_dims if any(dim in name for name in dim_names)]
    
    print(f"  🎯 Found {len(found_dims)}/{len(expected_dims)} expected dimensions: {found_dims}")
    assert len(found_dims) >= 2, f"Should find at least 2 expected dimensions, found: {found_dims}"
    
    print("✅ E-commerce business logic test passed!")
    return score >= 70  # Pass if score is 70% or higher


def test_pharma_business_logic():
    """Test business logic for pharmaceutical STTM."""
    print("=" * 50)
    print("💊 PHARMACEUTICAL BUSINESS LOGIC TEST")
    print("=" * 50)
    
    # Generate STTM
    agent = AnalystAgent()
    data_dir = Path(__file__).parent.parent / "data" / "Pharma"
    file_paths = [str(f) for f in data_dir.glob("*.csv")]
    sttm = agent.generate_sttm(file_paths)
    
    # Analyze business logic
    score, max_score = analyze_sttm_business_logic(sttm, "Pharmaceutical")
    
    # Specific pharmaceutical validations
    fact_tables = [t for t in sttm.tables if t.table_type == "fact"]
    dim_tables = [t for t in sttm.tables if t.table_type == "dimension"]
    
    # Validate pharmaceutical specific requirements
    assert len(fact_tables) >= 1, "Pharmaceutical data should have at least one fact table"
    
    # Check for time-based analysis (common in pharma)
    time_columns = []
    for table in sttm.tables:
        for col in table.columns:
            if any(time_word in col.source_column.lower() for time_word in ["date", "time", "hour", "day", "week", "month"]):
                time_columns.append(col.source_column)
    
    print(f"  🕒 Found {len(time_columns)} time-related columns for temporal analysis")
    assert len(time_columns) >= 1, "Pharmaceutical data should have time-based columns"
    
    print("✅ Pharmaceutical business logic test passed!")
    return score >= 60  # Pass if score is 60% or higher


def test_data_quality_indicators():
    """Test data quality indicators in the STTM."""
    print("=" * 50)
    print("🔍 DATA QUALITY INDICATORS TEST")
    print("=" * 50)
    
    # Generate STTM
    agent = AnalystAgent()
    data_dir = Path(__file__).parent.parent / "data" / "ecommerce"
    file_paths = [str(f) for f in data_dir.glob("*.csv")]
    sttm = agent.generate_sttm(file_paths)
    
    quality_indicators = []
    
    # Check for surrogate keys (good practice)
    for table in sttm.tables:
        for col in table.columns:
            if col.source_column == col.target_column and "key" in col.target_column.lower():
                quality_indicators.append(f"✅ Surrogate key pattern: {col.target_column}")
    
    # Check for consistent naming
    naming_patterns = []
    for table in sttm.tables:
        if table.table_type == "fact" and not table.target_table_name.startswith("fact_"):
            naming_patterns.append(f"⚠️  Fact table naming: {table.target_table_name}")
        elif table.table_type == "dimension" and not table.target_table_name.startswith("dim_"):
            naming_patterns.append(f"⚠️  Dimension table naming: {table.target_table_name}")
    
    # Check for proper data types
    type_quality = []
    for table in sttm.tables:
        for col in table.columns:
            col_name = col.source_column.lower()
            target_type = col.target_type
            
            if "id" in col_name and "StringType" in target_type:
                type_quality.append(f"⚠️  ID as string: {col.source_column}")
            elif "amount" in col_name and "IntegerType" in target_type:
                type_quality.append(f"⚠️  Amount as integer: {col.source_column}")
    
    print("  📊 Data Quality Analysis:")
    print(f"    {len(quality_indicators)} positive indicators")
    print(f"    {len(naming_patterns)} naming issues")
    print(f"    {len(type_quality)} type issues")
    
    # Calculate quality score
    total_issues = len(naming_patterns) + len(type_quality)
    quality_score = max(0, 100 - total_issues * 10)
    
    print(f"  🎯 Data Quality Score: {quality_score}/100")
    
    return quality_score >= 70


def run_business_logic_tests():
    """Run all business logic tests."""
    print("🚀 Running Business Logic Accuracy Tests")
    print("=" * 60)
    
    tests = [
        ("E-commerce Business Logic", test_ecommerce_business_logic),
        ("Pharma Business Logic", test_pharma_business_logic),
        ("Data Quality Indicators", test_data_quality_indicators),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            print(f"\n{'='*20} {test_name} {'='*20}")
            test_func()
            results.append((test_name, "PASS"))
        except Exception as e:
            print(f"❌ {test_name} failed: {str(e)}")
            results.append((test_name, f"FAIL: {str(e)}"))
    
    # Print summary
    print(f"\n{'='*60}")
    print("📊 BUSINESS LOGIC TEST RESULTS")
    print("=" * 60)
    
    for test_name, result in results:
        status_icon = "✅" if "PASS" in result else "❌"
        print(f"{status_icon} {test_name}: {result}")
    
    passed_tests = sum(1 for _, result in results if "PASS" in result)
    total_tests = len(results)
    
    print(f"\n🎯 Business Logic Accuracy: {passed_tests}/{total_tests} tests passed ({passed_tests/total_tests*100:.1f}%)")
    
    if passed_tests == total_tests:
        print("🎉 All business logic tests passed! Analyst Agent follows good practices.")
    else:
        print("⚠️  Some business logic tests failed. Review the results above.")
    
    return passed_tests == total_tests


if __name__ == "__main__":
    run_business_logic_tests() 