# src/nuvyn_bldr/schemas/code_validation.py

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class ValidationIssue(BaseModel):
    """A validation issue found in generated code."""
    issue_type: str = Field(..., description="Type of issue: syntax, logic, performance")
    severity: str = Field(..., description="Severity: error, warning, info")
    message: str = Field(..., description="Description of the issue")
    line_number: Optional[int] = Field(None, description="Line number where issue occurs")
    suggestion: Optional[str] = Field(None, description="Suggested fix")

class ValidationResult(BaseModel):
    """Result of code validation."""
    is_valid: bool = Field(..., description="Whether the code is valid")
    issues: List[ValidationIssue] = Field(default_factory=list, description="List of validation issues")
    score: float = Field(..., description="Validation score (0-100)")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Validation metadata") 