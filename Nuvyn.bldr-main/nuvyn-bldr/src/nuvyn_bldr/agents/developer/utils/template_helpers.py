# src/nuvyn_bldr/agents/developer/utils/template_helpers.py

"""
Template helper functions for Jinja2 templates.
"""

class TemplateHelpers:
    """Helper functions for Jinja2 templates."""
    
    @staticmethod
    def format_transformation_rule(rule: str) -> str:
        """Format transformation rule for PySpark code."""
        # Placeholder implementation
        return rule
    
    @staticmethod
    def generate_column_mapping(column_data: dict) -> str:
        """Generate column mapping code."""
        # Placeholder implementation
        return f"col('{column_data.get('source_column', '')}')" 