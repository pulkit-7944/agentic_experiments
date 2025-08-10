# src/nuvyn_bldr/schemas/data_quality_suite.py

from pydantic import BaseModel, Field
from typing import List, Dict, Any

class ValidationRule(BaseModel):
    """A data quality validation rule."""
    rule_name: str = Field(..., description="Name of the validation rule")
    rule_type: str = Field(..., description="Type of validation")
    rule_definition: str = Field(..., description="Rule definition")
    severity: str = Field("error", description="Severity level: error, warning, info")

class DataQualitySuite(BaseModel):
    """Complete data quality validation suite."""
    table_name: str = Field(..., description="Table being validated")
    validation_rules: List[ValidationRule] = Field(..., description="List of validation rules")
    validation_code: str = Field(..., description="Generated validation code")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Validation metadata") 