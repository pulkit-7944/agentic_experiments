# src/nuvyn_bldr/agents/developer/generators/base_generator.py

"""
Base generator class for code generation components.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseGenerator(ABC):
    """Base class for all code generators."""
    
    @abstractmethod
    def generate(self, data: Dict[str, Any]) -> str:
        """Generate code from input data."""
        pass 