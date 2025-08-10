#!/usr/bin/env python3
"""
Simple accuracy test for Analyst Agent.

This test focuses on the core functionality and validates that the Analyst Agent
can generate valid STTM documents that follow basic star schema principles.
"""

import json
import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from nuvyn_bldr.agents.analyst.agent import AnalystAgent
from nuvyn_bldr.schemas.sttm import STTM


def validate_housekeeping_columns(table):
    """Validate that a table has all required housekeeping columns."""
    housekeeping_columns = {
        'created_timestamp': {'type': 'TimestampType()', 'rule': 'current_timestamp()'},
        'updated_timestamp': {'type': 'TimestampType()', 'rule': 'current_timestamp()'},
        'source_system': {'type': 'StringType()', 'rule': 'literal'},
        'batch_id': {'type': 'StringType()', 'rule': 'literal'},
        'is_active': {'type': 'BooleanType()', 'rule': 'literal(true)'},
        'version': {'type': 'IntegerType()', 'rule': 'literal(1)'}
    }
    
    table_columns = {col.target_column: col for col in table.columns}
    
    missing_columns = []
    for col_name, expected in housekeeping_columns.items():
        if col_name not in table_columns:
            missing_columns.append(col_name)
        else:
            col = table_columns[col_name]
            # Validate type
            if expected['type'] not in col.target_type:
                print(f"  ⚠️  Housekeeping column {col_name} has wrong type: {col.target_type} (expected {expected['type']})")
            # Validate transformation rule
            if expected['rule'] not in col.transformation_rule:
                print(f"  ⚠️  Housekeeping column {col_name} has wrong rule: {col.transformation_rule} (expected {expected['rule']})")
    
    if missing_columns:
        print(f"  ❌ Missing housekeeping columns: {missing_columns}")
        return False
    else:
        print(f"  ✅ All housekeeping columns present in {table.target_table_name}")
        return True


def validate_null_handling(table):
    """Validate that all columns use coalesce() for null handling."""
    non_housekeeping_columns = [
        col for col in table.columns 
        if col.target_column not in ['created_timestamp', 'updated_timestamp', 'source_system', 'batch_id', 'is_active', 'version']
    ]
    
    columns_without_coalesce = []
    for col in non_housekeeping_columns:
        if col.source_column and 'coalesce(' not in col.transformation_rule:
            columns_without_coalesce.append(col.target_column)
    
    if columns_without_coalesce:
        print(f"  ⚠️  Columns without coalesce() null handling: {columns_without_coalesce}")
        return False
    else:
        print(f"  ✅ All columns in {table.target_table_name} use coalesce() for null handling")
        return True


def test_ecommerce_accuracy():
    """Test the accuracy of STTM generation for e-commerce data."""
    print("🔍 Testing E-commerce STTM Accuracy...")
    
    # Initialize agent
    agent = AnalystAgent()
    
    # Generate STTM
    data_dir = Path(__file__).parent.parent / "data" / "ecommerce"
    file_paths = [str(f) for f in data_dir.glob("*.csv")]
    sttm = agent.generate_sttm(file_paths)
    
    # Basic validation
    assert sttm.sttm_id is not None, "STTM should have an ID"
    assert sttm.description is not None, "STTM should have a description"
    assert len(sttm.tables) > 0, "STTM should have tables"
    
    # Find fact and dimension tables
    fact_tables = [t for t in sttm.tables if t.table_type == "fact"]
    dim_tables = [t for t in sttm.tables if t.table_type == "dimension"]
    
    print(f"  📊 Found {len(fact_tables)} fact table(s) and {len(dim_tables)} dimension table(s)")
    
    # Validate fact table
    assert len(fact_tables) >= 1, "Should have at least one fact table"
    fact_table = fact_tables[0]
    
    # Check for expected columns in fact table
    fact_columns = [col.source_column for col in fact_table.columns]
    expected_fact_columns = ["sale_id", "customer_id", "product_id", "store_id"]
    found_columns = [col for col in expected_fact_columns if col in fact_columns]
    
    print(f"  ✅ Found {len(found_columns)}/{len(expected_fact_columns)} expected fact table columns: {found_columns}")
    assert len(found_columns) >= 3, f"Should find at least 3 expected columns, found: {found_columns}"
    
    # Check primary key
    assert len(fact_table.primary_key) > 0, "Fact table should have a primary key"
    print(f"  ✅ Fact table has primary key: {fact_table.primary_key}")
    
    # Check foreign keys
    if fact_table.foreign_keys:
        print(f"  ✅ Fact table has {len(fact_table.foreign_keys)} foreign keys")
    else:
        print("  ⚠️  Fact table has no foreign keys (this might be expected)")
    
    # Validate housekeeping columns for fact table
    validate_housekeeping_columns(fact_table)
    
    # Validate null handling for fact table
    validate_null_handling(fact_table)
    
    # Validate dimension tables
    assert len(dim_tables) >= 1, "Should have at least one dimension table"
    
    for dim_table in dim_tables:
        assert len(dim_table.primary_key) > 0, f"Dimension table {dim_table.target_table_name} should have a primary key"
        print(f"  ✅ Dimension table '{dim_table.target_table_name}' has primary key: {dim_table.primary_key}")
        
        # Validate housekeeping columns for dimension table
        validate_housekeeping_columns(dim_table)
        
        # Validate null handling for dimension table
        validate_null_handling(dim_table)
    
    print("✅ E-commerce accuracy test passed!")
    return True


def test_pharma_accuracy():
    """Test the accuracy of STTM generation for pharmaceutical data."""
    print("🔍 Testing Pharma STTM Accuracy...")
    
    # Initialize agent
    agent = AnalystAgent()
    
    # Generate STTM
    data_dir = Path(__file__).parent.parent / "data" / "Pharma"
    file_paths = [str(f) for f in data_dir.glob("*.csv")]
    sttm = agent.generate_sttm(file_paths)
    
    # Basic validation
    assert sttm.sttm_id is not None, "STTM should have an ID"
    assert sttm.description is not None, "STTM should have a description"
    assert len(sttm.tables) > 0, "STTM should have tables"
    
    # Find fact and dimension tables
    fact_tables = [t for t in sttm.tables if t.table_type == "fact"]
    dim_tables = [t for t in sttm.tables if t.table_type == "dimension"]
    
    print(f"  📊 Found {len(fact_tables)} fact table(s) and {len(dim_tables)} dimension table(s)")
    
    # Should have at least one fact table
    assert len(fact_tables) >= 1, "Should have at least one fact table"
    
    # Should have dimension tables
    assert len(dim_tables) >= 1, "Should have at least one dimension table"
    
    # Check for time-based columns (common in pharmaceutical data)
    time_columns = []
    for table in sttm.tables:
        for col in table.columns:
            if col.source_column and any(time_word in col.source_column.lower() for time_word in ["date", "time", "hour", "day", "week", "month"]):
                time_columns.append(col.source_column)
    
    print(f"  ✅ Found {len(time_columns)} time-related columns: {time_columns[:5]}...")
    
    # Validate primary keys and housekeeping columns
    for table in sttm.tables:
        assert len(table.primary_key) > 0, f"Table {table.target_table_name} should have a primary key"
        
        # Validate housekeeping columns
        validate_housekeeping_columns(table)
        
        # Validate null handling
        validate_null_handling(table)
    
    print("✅ Pharma accuracy test passed!")
    return True


def test_schema_validation():
    """Test that generated STTM validates against the schema."""
    print("🔍 Testing Schema Validation...")
    
    # Initialize agent
    agent = AnalystAgent()
    
    # Generate STTM
    data_dir = Path(__file__).parent.parent / "data" / "ecommerce"
    file_paths = [str(f) for f in data_dir.glob("*.csv")]
    sttm = agent.generate_sttm(file_paths)
    
    # Test serialization
    sttm_json = sttm.model_dump_json()
    sttm_dict = sttm.model_dump()
    
    # Test deserialization
    sttm_recreated = STTM.model_validate_json(sttm_json)
    
    # Verify consistency
    assert sttm_recreated.sttm_id == sttm.sttm_id, "STTM ID should be preserved"
    assert len(sttm_recreated.tables) == len(sttm.tables), "Number of tables should be preserved"
    
    print("✅ Schema validation test passed!")
    return True


def test_performance():
    """Test performance of STTM generation."""
    print("🔍 Testing Performance...")
    
    import time
    
    # Initialize agent
    agent = AnalystAgent()
    
    # Generate STTM and measure time
    data_dir = Path(__file__).parent.parent / "data" / "ecommerce"
    file_paths = [str(f) for f in data_dir.glob("*.csv")]
    
    start_time = time.time()
    sttm = agent.generate_sttm(file_paths)
    end_time = time.time()
    
    generation_time = end_time - start_time
    
    print(f"  ⏱️  STTM generation took {generation_time:.2f} seconds")
    
    # Should complete within 60 seconds (more lenient for LLM calls)
    assert generation_time < 60, f"STTM generation took too long: {generation_time:.2f} seconds"
    
    print("✅ Performance test passed!")
    return True


def run_simple_accuracy_tests():
    """Run all simple accuracy tests."""
    print("🚀 Running Simple Analyst Agent Accuracy Tests")
    print("=" * 60)
    
    tests = [
        ("E-commerce Accuracy", test_ecommerce_accuracy),
        ("Pharma Accuracy", test_pharma_accuracy),
        ("Schema Validation", test_schema_validation),
        ("Performance", test_performance),
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
    print("📊 SIMPLE ACCURACY TEST RESULTS")
    print("=" * 60)
    
    for test_name, result in results:
        status_icon = "✅" if "PASS" in result else "❌"
        print(f"{status_icon} {test_name}: {result}")
    
    passed_tests = sum(1 for _, result in results if "PASS" in result)
    total_tests = len(results)
    
    print(f"\n🎯 Overall Accuracy: {passed_tests}/{total_tests} tests passed ({passed_tests/total_tests*100:.1f}%)")
    
    if passed_tests == total_tests:
        print("🎉 All simple accuracy tests passed! Analyst Agent is working correctly.")
    else:
        print("⚠️  Some tests failed. Review the results above.")
    
    return passed_tests == total_tests


if __name__ == "__main__":
    run_simple_accuracy_tests() 