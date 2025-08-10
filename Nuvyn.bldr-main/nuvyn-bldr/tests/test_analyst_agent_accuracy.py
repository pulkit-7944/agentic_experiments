#!/usr/bin/env python3
"""
Test suite for Analyst Agent accuracy validation.

This test suite validates the accuracy of the Analyst Agent in generating
Source-to-Target Mapping (STTM) documents by checking:

1. Schema compliance
2. Business logic accuracy
3. Data type mapping correctness
4. Star schema design principles
5. Foreign key relationship accuracy
"""

import json
import pytest
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, List
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from nuvyn_bldr.agents.analyst.agent import AnalystAgent
from nuvyn_bldr.schemas.sttm import STTM
from nuvyn_bldr.core.error_handler import STTMGenerationError


class TestAnalystAgentAccuracy:
    """Test suite for Analyst Agent accuracy validation."""
    
    @pytest.fixture
    def analyst_agent(self):
        """Create an Analyst Agent instance for testing."""
        return AnalystAgent()
    
    @pytest.fixture
    def test_data_dir(self):
        """Get the test data directory."""
        return Path(__file__).parent.parent / "data"
    
    def test_ecommerce_sttm_accuracy(self, analyst_agent, test_data_dir):
        """Test STTM accuracy for e-commerce dataset."""
        ecommerce_dir = test_data_dir / "ecommerce"
        
        # Generate STTM
        file_paths = [str(f) for f in ecommerce_dir.glob("*.csv")]
        sttm = analyst_agent.generate_sttm(file_paths)
        
        # Validate basic structure
        assert sttm.sttm_id is not None
        assert sttm.description is not None
        assert len(sttm.tables) > 0
        
        # Validate table structure
        self._validate_ecommerce_tables(sttm.tables)
        
        # Validate star schema principles
        self._validate_star_schema_principles(sttm.tables)
        
        # Validate data type mappings
        self._validate_data_type_mappings(sttm.tables)
        
        # Validate housekeeping columns
        self._validate_housekeeping_columns(sttm.tables)
        
        # Validate null handling
        self._validate_null_handling(sttm.tables)
        
        print("✅ E-commerce STTM accuracy test passed")
    
    def test_pharma_sttm_accuracy(self, analyst_agent, test_data_dir):
        """Test STTM accuracy for pharmaceutical dataset."""
        pharma_dir = test_data_dir / "Pharma"
        
        # Generate STTM
        file_paths = [str(f) for f in pharma_dir.glob("*.csv")]
        sttm = analyst_agent.generate_sttm(file_paths)
        
        # Validate basic structure
        assert sttm.sttm_id is not None
        assert sttm.description is not None
        assert len(sttm.tables) > 0
        
        # Validate table structure
        self._validate_pharma_tables(sttm.tables)
        
        # Validate star schema principles
        self._validate_star_schema_principles(sttm.tables)
        
        # Validate housekeeping columns
        self._validate_housekeeping_columns(sttm.tables)
        
        # Validate null handling
        self._validate_null_handling(sttm.tables)
        
        print("✅ Pharma STTM accuracy test passed")
    
    def test_stock_sttm_accuracy(self, analyst_agent, test_data_dir):
        """Test STTM accuracy for stock trading dataset."""
        stock_dir = test_data_dir / "stock"
        
        # Generate STTM
        file_paths = [str(f) for f in stock_dir.glob("*.csv")]
        sttm = analyst_agent.generate_sttm(file_paths)
        
        # Validate basic structure
        assert sttm.sttm_id is not None
        assert sttm.description is not None
        assert len(sttm.tables) > 0
        
        # Validate table structure
        self._validate_stock_tables(sttm.tables)
        
        # Validate star schema principles
        self._validate_star_schema_principles(sttm.tables)
        
        # Validate housekeeping columns
        self._validate_housekeeping_columns(sttm.tables)
        
        # Validate null handling
        self._validate_null_handling(sttm.tables)
        
        print("✅ Stock STTM accuracy test passed")
    
    def _validate_ecommerce_tables(self, tables: List[Dict[str, Any]]):
        """Validate e-commerce specific table structure and relationships."""
        
        # Find fact and dimension tables
        fact_tables = [t for t in tables if t.table_type == "fact"]
        dim_tables = [t for t in tables if t.table_type == "dimension"]
        
        # Should have exactly one fact table
        assert len(fact_tables) == 1, f"Expected 1 fact table, got {len(fact_tables)}"
        fact_table = fact_tables[0]
        
        # Validate fact table structure
        assert "sale_id" in [col.source_column for col in fact_table.columns], "Missing sale_id in fact table"
        assert "customer_id" in [col.source_column for col in fact_table.columns], "Missing customer_id in fact table"
        assert "product_id" in [col.source_column for col in fact_table.columns], "Missing product_id in fact table"
        assert "store_id" in [col.source_column for col in fact_table.columns], "Missing store_id in fact table"
        
        # Validate primary key
        assert "sale_id" in fact_table.primary_key, "sale_id should be primary key"
        
        # Validate foreign keys
        assert fact_table.foreign_keys is not None, "Fact table should have foreign keys"
        expected_fks = ["customer_key", "product_key", "store_key"]
        for fk in expected_fks:
            assert fk in fact_table.foreign_keys, f"Missing foreign key: {fk}"
        
        # Validate dimension tables
        dim_names = [t.target_table_name for t in dim_tables]
        assert "dim_customer" in dim_names, "Missing customer dimension"
        assert "dim_product" in dim_names, "Missing product dimension"
        assert "dim_store" in dim_names, "Missing store dimension"
        
        # Validate dimension table primary keys
        for dim_table in dim_tables:
            assert len(dim_table.primary_key) > 0, f"Dimension table {dim_table.target_table_name} missing primary key"
    
    def _validate_pharma_tables(self, tables: List[Dict[str, Any]]):
        """Validate pharmaceutical specific table structure and relationships."""
        
        # Find fact and dimension tables
        fact_tables = [t for t in tables if t.table_type == "fact"]
        dim_tables = [t for t in tables if t.table_type == "dimension"]
        
        # Should have at least one fact table
        assert len(fact_tables) >= 1, f"Expected at least 1 fact table, got {len(fact_tables)}"
        
        # Validate that we have time-based dimensions
        time_dimensions = [t for t in dim_tables if any("time" in col.target_column.lower() or "date" in col.target_column.lower() for col in t.columns)]
        assert len(time_dimensions) >= 1, "Should have time-based dimensions for pharmaceutical data"
        
        # Validate primary keys exist
        for table in tables:
            assert len(table.primary_key) > 0, f"Table {table.target_table_name} missing primary key"
    
    def _validate_stock_tables(self, tables: List[Dict[str, Any]]):
        """Validate stock trading specific table structure and relationships."""
        
        # Find fact and dimension tables
        fact_tables = [t for t in tables if t.table_type == "fact"]
        dim_tables = [t for t in tables if t.table_type == "dimension"]
        
        # Should have at least one fact table
        assert len(fact_tables) >= 1, f"Expected at least 1 fact table, got {len(fact_tables)}"
        
        # Validate security dimension exists
        security_dims = [t for t in dim_tables if "security" in t.target_table_name.lower()]
        assert len(security_dims) >= 1, "Should have security dimension for stock data"
        
        # Validate time dimension exists
        time_dims = [t for t in dim_tables if any("time" in col.target_column.lower() or "date" in col.target_column.lower() for col in t.columns)]
        assert len(time_dims) >= 1, "Should have time dimension for stock data"
    
    def _validate_star_schema_principles(self, tables: List[Dict[str, Any]]):
        """Validate star schema design principles."""
        
        fact_tables = [t for t in tables if t.table_type == "fact"]
        dim_tables = [t for t in tables if t.table_type == "dimension"]
        
        # Should have at least one fact table
        assert len(fact_tables) >= 1, "Star schema should have at least one fact table"
        
        # Should have dimension tables
        assert len(dim_tables) >= 1, "Star schema should have dimension tables"
        
        # Fact tables should have foreign keys
        for fact_table in fact_tables:
            assert fact_table.foreign_keys is not None, f"Fact table {fact_table.target_table_name} should have foreign keys"
            assert len(fact_table.foreign_keys) > 0, f"Fact table {fact_table.target_table_name} should have foreign keys"
        
        # Dimension tables should not have foreign keys (in simple star schema)
        for dim_table in dim_tables:
            if hasattr(dim_table, 'foreign_keys') and dim_table.foreign_keys:
                # Only allow foreign keys for conformed dimensions
                assert "conformed" in dim_table.target_table_name.lower() or "shared" in dim_table.target_table_name.lower(), f"Dimension table {dim_table.target_table_name} should not have foreign keys unless it's a conformed dimension"
    
    def _validate_data_type_mappings(self, tables: List[Dict[str, Any]]):
        """Validate data type mappings are appropriate."""
        
        for table in tables:
            for column in table.columns:
                source_type = column.source_column
                target_type = column.target_type
                
                # Validate numeric types
                if any(numeric in source_type.lower() for numeric in ["id", "price", "amount", "quantity", "count"]):
                    assert any(numeric in target_type for numeric in ["IntegerType", "DoubleType", "LongType"]), f"Inappropriate type mapping for {source_type}: {target_type}"
                
                # Validate string types
                if any(text in source_type.lower() for text in ["name", "description", "address", "email", "phone"]):
                    assert "StringType" in target_type, f"Inappropriate type mapping for {source_type}: {target_type}"
                
                # Validate date/time types
                if any(time in source_type.lower() for time in ["date", "time", "datetime"]):
                    assert any(time in target_type for time in ["TimestampType", "DateType"]), f"Inappropriate type mapping for {source_type}: {target_type}"
                
                # Validate boolean types
                if any(bool_flag in source_type.lower() for bool_flag in ["is_", "has_", "flag"]):
                    assert "BooleanType" in target_type, f"Inappropriate type mapping for {source_type}: {target_type}"
    
    def _validate_housekeeping_columns(self, tables: List[Dict[str, Any]]):
        """Validate that all tables have required housekeeping columns."""
        
        housekeeping_columns = {
            'created_timestamp': {'type': 'TimestampType()', 'rule': 'current_timestamp()'},
            'updated_timestamp': {'type': 'TimestampType()', 'rule': 'current_timestamp()'},
            'source_system': {'type': 'StringType()', 'rule': 'literal'},
            'batch_id': {'type': 'StringType()', 'rule': 'literal'},
            'is_active': {'type': 'BooleanType()', 'rule': 'literal(true)'},
            'version': {'type': 'IntegerType()', 'rule': 'literal(1)'}
        }
        
        for table in tables:
            table_columns = {col.target_column: col for col in table.columns}
            
            for col_name, expected in housekeeping_columns.items():
                assert col_name in table_columns, f"Table {table.target_table_name} missing housekeeping column: {col_name}"
                
                col = table_columns[col_name]
                assert expected['type'] in col.target_type, f"Housekeeping column {col_name} in {table.target_table_name} has wrong type: {col.target_type}"
                assert expected['rule'] in col.transformation_rule, f"Housekeeping column {col_name} in {table.target_table_name} has wrong rule: {col.transformation_rule}"
    
    def _validate_null_handling(self, tables: List[Dict[str, Any]]):
        """Validate that all columns use coalesce() for null handling."""
        
        for table in tables:
            non_housekeeping_columns = [
                col for col in table.columns 
                if col.target_column not in ['created_timestamp', 'updated_timestamp', 'source_system', 'batch_id', 'is_active', 'version']
            ]
            
            for col in non_housekeeping_columns:
                if col.source_column:  # Only check columns with source data
                    assert 'coalesce(' in col.transformation_rule, f"Column {col.target_column} in {table.target_table_name} missing coalesce() null handling: {col.transformation_rule}"
    
    def test_sttm_schema_validation(self, analyst_agent, test_data_dir):
        """Test that generated STTM validates against the schema."""
        ecommerce_dir = test_data_dir / "ecommerce"
        file_paths = [str(f) for f in ecommerce_dir.glob("*.csv")]
        
        sttm = analyst_agent.generate_sttm(file_paths)
        
        # Convert to dict and back to test serialization
        sttm_dict = sttm.model_dump()
        sttm_json = sttm.model_dump_json()
        
        # Validate we can recreate the object
        sttm_recreated = STTM.model_validate_json(sttm_json)
        assert sttm_recreated.sttm_id == sttm.sttm_id
        
        print("✅ STTM schema validation test passed")
    
    def test_error_handling(self, analyst_agent):
        """Test error handling for invalid inputs."""
        
        # Test with non-existent files
        with pytest.raises(STTMGenerationError):
            analyst_agent.generate_sttm(["non_existent_file.csv"])
        
        # Test with empty file list
        with pytest.raises(STTMGenerationError):
            analyst_agent.generate_sttm([])
        
        print("✅ Error handling test passed")
    
    def test_performance_benchmark(self, analyst_agent, test_data_dir):
        """Test performance of STTM generation."""
        import time
        
        ecommerce_dir = test_data_dir / "ecommerce"
        file_paths = [str(f) for f in ecommerce_dir.glob("*.csv")]
        
        # Measure generation time
        start_time = time.time()
        sttm = analyst_agent.generate_sttm(file_paths)
        end_time = time.time()
        
        generation_time = end_time - start_time
        
        # Should complete within reasonable time (30 seconds)
        assert generation_time < 30, f"STTM generation took too long: {generation_time:.2f} seconds"
        
        print(f"✅ Performance test passed: {generation_time:.2f} seconds")
    
    def test_consistency_across_runs(self, analyst_agent, test_data_dir):
        """Test that STTM generation is consistent across multiple runs."""
        ecommerce_dir = test_data_dir / "ecommerce"
        file_paths = [str(f) for f in ecommerce_dir.glob("*.csv")]
        
        # Generate STTM twice
        sttm1 = analyst_agent.generate_sttm(file_paths)
        sttm2 = analyst_agent.generate_sttm(file_paths)
        
        # Compare structure (ignore timestamps and IDs that might change)
        assert len(sttm1.tables) == len(sttm2.tables), "Number of tables should be consistent"
        
        # Compare table types
        table_types1 = [t.table_type for t in sttm1.tables]
        table_types2 = [t.table_type for t in sttm2.tables]
        assert table_types1 == table_types2, "Table types should be consistent"
        
        print("✅ Consistency test passed")


def run_accuracy_tests():
    """Run all accuracy tests and provide a summary report."""
    print("🚀 Running Analyst Agent Accuracy Tests")
    print("=" * 50)
    
    # Create test instance and objects directly
    test_instance = TestAnalystAgentAccuracy()
    analyst_agent = AnalystAgent()
    test_data_dir = Path(__file__).parent.parent / "data"
    
    test_results = []
    
    # Run individual tests
    try:
        test_instance.test_ecommerce_sttm_accuracy(analyst_agent, test_data_dir)
        test_results.append(("E-commerce STTM", "PASS"))
    except Exception as e:
        test_results.append(("E-commerce STTM", f"FAIL: {str(e)}"))
    
    try:
        test_instance.test_pharma_sttm_accuracy(analyst_agent, test_data_dir)
        test_results.append(("Pharma STTM", "PASS"))
    except Exception as e:
        test_results.append(("Pharma STTM", f"FAIL: {str(e)}"))
    
    try:
        test_instance.test_stock_sttm_accuracy(analyst_agent, test_data_dir)
        test_results.append(("Stock STTM", "PASS"))
    except Exception as e:
        test_results.append(("Stock STTM", f"FAIL: {str(e)}"))
    
    try:
        test_instance.test_sttm_schema_validation(analyst_agent, test_data_dir)
        test_results.append(("Schema Validation", "PASS"))
    except Exception as e:
        test_results.append(("Schema Validation", f"FAIL: {str(e)}"))
    
    try:
        test_instance.test_performance_benchmark(analyst_agent, test_data_dir)
        test_results.append(("Performance", "PASS"))
    except Exception as e:
        test_results.append(("Performance", f"FAIL: {str(e)}"))
    
    try:
        test_instance.test_consistency_across_runs(analyst_agent, test_data_dir)
        test_results.append(("Consistency", "PASS"))
    except Exception as e:
        test_results.append(("Consistency", f"FAIL: {str(e)}"))
    
    # Print summary
    print("\n📊 Test Results Summary:")
    print("=" * 50)
    for test_name, result in test_results:
        status_icon = "✅" if "PASS" in result else "❌"
        print(f"{status_icon} {test_name}: {result}")
    
    passed_tests = sum(1 for _, result in test_results if "PASS" in result)
    total_tests = len(test_results)
    
    print(f"\n🎯 Overall Accuracy: {passed_tests}/{total_tests} tests passed ({passed_tests/total_tests*100:.1f}%)")
    
    if passed_tests == total_tests:
        print("🎉 All accuracy tests passed! Analyst Agent is working correctly.")
    else:
        print("⚠️  Some tests failed. Review the results above.")


if __name__ == "__main__":
    run_accuracy_tests() 