# src/nuvyn_bldr/schemas/databricks_notebook.py

from pydantic import BaseModel, Field
from typing import List, Dict, Any

class NotebookCell(BaseModel):
    """Represents a cell in a Databricks notebook."""
    cell_type: str = Field(..., description="Cell type: code or markdown")
    source: str = Field(..., description="Cell content")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Cell metadata")

class DatabricksNotebook(BaseModel):
    """Databricks notebook structure."""
    name: str = Field(..., description="Notebook name")
    language: str = Field("python", description="Notebook language")
    cells: List[NotebookCell] = Field(..., description="List of notebook cells")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Notebook metadata") 