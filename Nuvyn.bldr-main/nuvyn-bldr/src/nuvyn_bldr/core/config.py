# src/nuvyn_bldr/config.py
print("DEBUG: Entered config.py")

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Dict, Optional, Any
from pathlib import Path

print("DEBUG: Defining Settings class...")
class Settings(BaseSettings):
    """
    Manages all application settings using Pydantic's BaseSettings.

    This class provides a centralized, type-safe way to handle configuration.
    It automatically reads settings from environment variables and a local .env
    file, validating them against the defined types. Environment variables
    will always take precedence over values in the .env file.

    This class is intended to be used as a singleton instance, `settings`,
    which is created at the end of this file.

    Attributes:
        LLM_PROVIDER (str): The primary LLM provider to use (e.g., 'groq').
        LLM_MODELS (Dict[str, str]): A mapping of logical aliases to actual model names.
        GROQ_API_KEY (str): The API key for the Groq service.
        OPENAI_API_KEY (Optional[str]): The optional API key for OpenAI.
        DATABRICKS_HOST (str): The URL for the Databricks workspace.
        DATABRICKS_CLIENT_ID (str): The Client ID for the Service Principal.
        DATABRICKS_CLIENT_SECRET (str): The Client Secret for the Service Principal.
        DATABRICKS_TENANT_ID (str): The Azure Tenant ID for authentication.

    Usage:
        To access any setting from another module in the application:

        >>> from src.nuvyn_bldr.config import settings
        >>>
        >>> # Access the configured LLM provider
        >>> provider = settings.LLM_PROVIDER
        >>> print(f"Using LLM provider: {provider}")
        >>>
        >>> # Access a secret key
        >>> api_key = settings.GROQ_API_KEY
        >>> # Note: Be careful not to print secrets in production logs.
    """
    
    # --- LLM Provider Configuration ---
    LLM_PROVIDER: str = Field(
        # default="groq", 
        default="ollama", 
        description="The primary LLM provider to use ('groq', 'openai', 'azure_openai', 'ollama')."
    )
    
    # --- Model Alias Configuration ---
    LLM_MODELS: Dict[str, str] = Field(
        default={
            "default": "llama3-70b-8192",
            "fast": "llama3-8b-8192",
            "evaluator": "llama3-70b-8192",
            "ollama_default": "llama3.1:8b",
            "ollama_fast": "llama3.1:8b"
        },
        description="Maps logical model aliases (e.g., 'default') to actual model names."
    )
    # Base DIR for reading data files
    BASE_DIR: Path = Field(
        default=Path(__file__).resolve().parent.parent,
        description="Base directory for the project, used to locate resources like prompt templates."
    )

    # --- API Keys ---
    # --- Groq Configuration ---
    GROQ_API_KEY: str = Field(..., description="API key for the Groq service.")
    GROQ_MODEL: str = Field(default="llama3.1-8b-instant", description="Groq model name")
    GROQ_MAX_TOKENS: int = Field(default=4000, description="Groq max tokens")
    GROQ_TEMPERATURE: float = Field(default=0.1, description="Groq temperature")
    
    # --- OpenAI Configuration ---
    OPENAI_API_KEY: Optional[str] = Field(None, description="API key for OpenAI service (optional).")
    OPENAI_MODEL: str = Field(default="gpt-4o-mini", description="OpenAI model name")
    OPENAI_MAX_TOKENS: int = Field(default=4000, description="OpenAI max tokens")
    OPENAI_TEMPERATURE: float = Field(default=0.1, description="OpenAI temperature")
    
    # --- Ollama Configuration ---
    OLLAMA_BASE_URL: str = Field(default="http://localhost:11434", description="Ollama base URL")
    OLLAMA_MODEL: str = Field(default="llama3.1:8b", description="Ollama model name")
    OLLAMA_MAX_TOKENS: int = Field(default=4000, description="Ollama max tokens")
    OLLAMA_TEMPERATURE: float = Field(default=0.1, description="Ollama temperature")
    
    # --- Azure OpenAI Configuration ---
    AZURE_OPENAI_API_KEY: Optional[str] = Field(None, description="Azure OpenAI API key")
    AZURE_OPENAI_ENDPOINT: Optional[str] = Field(None, description="Azure OpenAI endpoint")
    AZURE_OPENAI_API_VERSION: str = Field(default="2023-05-15", description="Azure OpenAI API version")
    AZURE_OPENAI_CHAT_DEPLOYMENT: str = Field(default="gpt-4", description="Azure OpenAI chat deployment")
    AZURE_OPENAI_EMBEDDING_DEPLOYMENT: str = Field(default="text-embedding-ada-002", description="Azure OpenAI embedding deployment")
    AZURE_OPENAI_MAX_TOKENS: int = Field(default=4000, description="Azure OpenAI max tokens")
    AZURE_OPENAI_TEMPERATURE: float = Field(default=0.1, description="Azure OpenAI temperature")
    
    # --- Analyst Agent Azure OpenAI Configuration ---
    ANALYST_AGENT_AZURE_OPENAI_API_KEY: Optional[str] = Field(None, description="Analyst Agent Azure OpenAI API key")
    ANALYST_AGENT_AZURE_OPENAI_ENDPOINT: Optional[str] = Field(None, description="Analyst Agent Azure OpenAI endpoint")
    ANALYST_AGENT_AZURE_OPENAI_API_VERSION: str = Field(default="2024-12-01-preview", description="Analyst Agent Azure OpenAI API version")
    ANALYST_AGENT_AZURE_OPENAI_CHAT_DEPLOYMENT: str = Field(default="gpt-4.1-mini", description="Analyst Agent Azure OpenAI chat deployment")
    ANALYST_AGENT_AZURE_OPENAI_MAX_TOKENS: int = Field(default=16000, description="Analyst Agent Azure OpenAI max tokens")
    ANALYST_AGENT_AZURE_OPENAI_TEMPERATURE: float = Field(default=0.1, description="Analyst Agent Azure OpenAI temperature")
    ANALYST_AGENT_LLM_PROVIDER: str = Field(default="azure_openai", description="LLM provider for Analyst Agent")
    
    # --- Developer Agent Azure OpenAI Configuration ---
    DEVELOPER_AGENT_AZURE_OPENAI_API_KEY: Optional[str] = Field(None, description="Developer Agent Azure OpenAI API key")
    DEVELOPER_AGENT_AZURE_OPENAI_ENDPOINT: Optional[str] = Field(None, description="Developer Agent Azure OpenAI endpoint")
    DEVELOPER_AGENT_AZURE_OPENAI_API_VERSION: str = Field(default="2024-12-01-preview", description="Developer Agent Azure OpenAI API version")
    DEVELOPER_AGENT_AZURE_OPENAI_CHAT_DEPLOYMENT: str = Field(default="gpt-4", description="Developer Agent Azure OpenAI chat deployment")
    DEVELOPER_AGENT_AZURE_OPENAI_MAX_TOKENS: int = Field(default=4000, description="Developer Agent Azure OpenAI max tokens")
    DEVELOPER_AGENT_AZURE_OPENAI_TEMPERATURE: float = Field(default=0.1, description="Developer Agent Azure OpenAI temperature")
    
    # --- Azure Storage Configuration ---
    AZURE_STORAGE_CONNECTION_STRING: Optional[str] = Field(None, description="Azure Storage connection string")
    AZURE_STORAGE_ACCOUNT_NAME: Optional[str] = Field(None, description="Azure Storage account name")
    AZURE_STORAGE_ACCOUNT_KEY: Optional[str] = Field(None, description="Azure Storage account key")
    AZURE_STORAGE_SAS_TOKEN: Optional[str] = Field(None, description="Azure Storage SAS token")
    
    # --- Developer Agent Configuration ---
    DEVELOPER_AGENT_PROVIDER: str = Field(default="azure_openai", description="LLM provider for Developer Agent")
    DATABRICKS_JOB_CONFIG: Dict[str, Any] = Field(default_factory=dict, description="Databricks job configuration")
    CODE_GENERATION_CONFIG: Dict[str, Any] = Field(default_factory=dict, description="Code generation settings")

    # --- Databricks Configuration ---
    DATABRICKS_HOST: str = Field(..., description="The URL for the Databricks workspace, e.g., 'https://your-workspace.databricks.net'.")
    DATABRICKS_CLIENT_ID: str = Field(..., description="The Client ID (or Application ID) of the Service Principal.")
    DATABRICKS_CLIENT_SECRET: str = Field(..., description="The Client Secret of the Service Principal.")
    DATABRICKS_TENANT_ID: str = Field(..., description="The Azure Tenant ID for Databricks authentication.")

    # --- General Application Configuration ---
    LOG_LEVEL: str = Field(default="INFO", description="The logging level for the application (e.g., DEBUG, INFO, WARNING).")

    # Pydantic model configuration
    model_config = SettingsConfigDict(
        # This tells Pydantic to load settings from a .env file
        # in addition to environment variables.
        env_file=str(Path(__file__).parent.parent.parent.parent / ".env"),
        env_file_encoding="utf-8",
        # Ignore any extra fields defined in the .env file that are not
        # part of this model.
        extra="ignore"
    )

# Create a single, importable instance of the settings.
# All other modules in our application will import this `settings` object.

print("DEBUG: Instantiating settings...")

try:
    settings = Settings()
except Exception as e:
    import traceback
    print("🔥 ERROR: Failed to load application settings in config.py")
    traceback.print_exc()
    raise e
