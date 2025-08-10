# src/nuvyn_bldr/core/error_handler.py

class NuvynBldrError(Exception):
    """Base exception class for all custom errors in the Nuvyn.bldr application."""
    def __init__(self, message="An error occurred in the Nuvyn.bldr application."):
        self.message = message
        super().__init__(self.message)

class ConfigurationError(NuvynBldrError):
    """Raised when there is an error in the application's configuration."""
    def __init__(self, message="Configuration error."):
        super().__init__(message)

class STTMValidationError(NuvynBldrError):
    """Raised when the generated STTM fails validation against its schema."""
    def __init__(self, message="STTM validation failed."):
        super().__init__(message)

class AgentExecutionError(NuvynBldrError):
    """Raised when an agent fails to complete its primary execution task."""
    def __init__(self, agent_name: str, message: str):
        full_message = f"Execution failed for agent '{agent_name}': {message}"
        super().__init__(full_message)

class DatabricksAPIError(NuvynBldrError):
    """Raised when an interaction with the Databricks API fails."""
    def __init__(self, message="Databricks API call failed."):
        super().__init__(message)

class KnowledgeBaseError(NuvynBldrError):
    """Raised for errors related to the Knowledge Base (vector store)."""
    def __init__(self, message="Knowledge Base operation failed."):
        super().__init__(message)

class STTMGenerationError(NuvynBldrError):
    """Raised when an error occurs during STTM generation."""
    def __init__(self, message="STTM generation failed.", llm_response: str = None):
        self.llm_response = llm_response
        super().__init__(message)

class ELTGenerationError(NuvynBldrError):
    """Raised when an error occurs during ELT pipeline generation."""
    def __init__(self, message="ELT pipeline generation failed.", llm_response: str = None):
        self.llm_response = llm_response
        super().__init__(message)

class CodeValidationError(NuvynBldrError):
    """Raised when generated code fails validation."""
    def __init__(self, message="Code validation failed.", validation_issues: list = None):
        self.validation_issues = validation_issues
        super().__init__(message)

class DeploymentError(NuvynBldrError):
    """Raised when deployment configuration generation fails."""
    def __init__(self, message="Deployment configuration generation failed.", config_data: dict = None):
        self.config_data = config_data
        super().__init__(message)

class DataQualityError(NuvynBldrError):
    """Raised when data quality generation fails."""
    def __init__(self, message="Data quality generation failed.", quality_data: dict = None):
        self.quality_data = quality_data
        super().__init__(message)