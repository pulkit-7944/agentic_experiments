# src/nuvyn_bldr/core/llm_service.py

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langchain_community.chat_models import ChatOllama
from nuvyn_bldr.core.config import settings
from typing import Any, Dict
import logging

# Set up logging for this module
logger = logging.getLogger(__name__)

class LLMService:
    """A service class for providing a model-agnostic interface to LLMs.

    This class uses a registry pattern to dynamically select and instantiate
    the correct LangChain chat model based on the application's configuration.
    It allows agents to request a model by a logical alias (e.g., 'default')
    and override parameters like temperature on a per-call basis.

    This class is intended to be used as a singleton instance, `llm_service`,
    which is created at the end of this file.

    Attributes:
        provider_name (str): The name of the configured LLM provider (e.g., 'groq').
        model_map (Dict[str, str]): A mapping of model aliases to actual model names.

    Usage:
        To get a model instance, import the singleton and call the get_model method:

        >>> from src.nuvyn_bldr.core.llm_service import llm_service
        >>>
        >>> # Get the default model with default temperature (0.0)
        >>> default_model = llm_service.get_model()
        >>> response = default_model.invoke("Tell me a fact.")
        >>>
        >>> # Get a specific model by alias with a different temperature
        >>> creative_model = llm_service.get_model(model_alias="fast", temperature=0.7)
        >>> creative_response = creative_model.invoke("Tell me a joke.")
    """

    # The registry maps provider names from config to LangChain classes.
    LLM_PROVIDER_REGISTRY: Dict[str, Any] = {
        "groq": ChatGroq,
        "openai": ChatOpenAI,
        "azure_openai": AzureChatOpenAI,
        "ollama": ChatOllama,
        # To add a new provider, add one line here.
    }

    def __init__(self):
        """Initializes the LLMService, loading configuration from central settings."""
        self.provider_name = settings.LLM_PROVIDER
        self.model_map = settings.LLM_MODELS
        logger.info(f"LLMService initialized for provider: '{self.provider_name}'")
    
    def set_provider(self, provider_name: str):
        """Dynamically change the LLM provider."""
        if provider_name not in self.LLM_PROVIDER_REGISTRY:
            raise ValueError(f"Unsupported LLM provider '{provider_name}'. Supported providers are: {list(self.LLM_PROVIDER_REGISTRY.keys())}")
        self.provider_name = provider_name
        logger.info(f"LLMService provider changed to: '{self.provider_name}'")

    def get_model(
        self,
        model_alias: str = "default",
        temperature: float = 0.0,
        **kwargs: Any
    ) -> BaseChatModel:
        """Gets a configured LLM instance based on a logical alias.

        This is the primary method agents will use. It abstracts away the details
        of model selection and instantiation.

        Args:
            model_alias: The logical name for the model (e.g., 'default', 'fast').
            temperature: The model temperature for the call. Defaults to 0.0 for
                deterministic output suitable for agentic work.
            **kwargs: Additional keyword arguments to pass to the model's
                constructor (e.g., `max_tokens`).

        Returns:
            An instance of a LangChain chat model, ready to be used.

        Raises:
            ValueError: If the configured provider in settings is not found in
                the registry, or if the requested model_alias is not defined
                in the settings.
        """
        logger.info(f"Requesting LLM with alias '{model_alias}' and temperature {temperature}.")

        # 1. Look up the correct class from our registry.
        llm_class = self.LLM_PROVIDER_REGISTRY.get(self.provider_name)

        if not llm_class:
            logger.error(f"Unsupported LLM provider '{self.provider_name}'.")
            raise ValueError(
                f"Unsupported LLM provider '{self.provider_name}'. "
                f"Supported providers are: {list(self.LLM_PROVIDER_REGISTRY.keys())}"
            )

        # 2. Get the provider-specific model name based on alias and provider
        model_name = self._get_provider_specific_model_name(model_alias)
        
        if not model_name:
            logger.error(f"Unknown model alias '{model_alias}' for provider '{self.provider_name}'.")
            raise ValueError(
                f"Unknown model alias '{model_alias}' for provider '{self.provider_name}'. "
                f"Known aliases are: {list(self.model_map.keys())}"
            )

        # 3. Prepare the model parameters based on provider
        model_params = {
            "temperature": temperature,
            **kwargs  # Allow any other valid LangChain parameters to be passed.
        }

        # 4. Add provider-specific configuration
        if self.provider_name == "groq":
            model_params.update({
                "model_name": model_name,
                "groq_api_key": settings.GROQ_API_KEY,
                "max_tokens": kwargs.get("max_tokens", settings.GROQ_MAX_TOKENS)
            })
        elif self.provider_name == "openai" and settings.OPENAI_API_KEY:
            model_params.update({
                "model_name": model_name,
                "openai_api_key": settings.OPENAI_API_KEY,
                "max_tokens": kwargs.get("max_tokens", settings.OPENAI_MAX_TOKENS)
            })
        elif self.provider_name == "azure_openai":
            # Use Developer Agent specific Azure OpenAI configuration if available, otherwise fall back to Analyst Agent or general config
            api_key = settings.DEVELOPER_AGENT_AZURE_OPENAI_API_KEY or settings.ANALYST_AGENT_AZURE_OPENAI_API_KEY or settings.AZURE_OPENAI_API_KEY
            endpoint = settings.DEVELOPER_AGENT_AZURE_OPENAI_ENDPOINT or settings.ANALYST_AGENT_AZURE_OPENAI_ENDPOINT or settings.AZURE_OPENAI_ENDPOINT
            deployment = settings.DEVELOPER_AGENT_AZURE_OPENAI_CHAT_DEPLOYMENT or settings.ANALYST_AGENT_AZURE_OPENAI_CHAT_DEPLOYMENT or settings.AZURE_OPENAI_CHAT_DEPLOYMENT
            api_version = settings.DEVELOPER_AGENT_AZURE_OPENAI_API_VERSION or settings.ANALYST_AGENT_AZURE_OPENAI_API_VERSION or settings.AZURE_OPENAI_API_VERSION
            max_tokens = kwargs.get("max_tokens", settings.DEVELOPER_AGENT_AZURE_OPENAI_MAX_TOKENS or settings.ANALYST_AGENT_AZURE_OPENAI_MAX_TOKENS or settings.AZURE_OPENAI_MAX_TOKENS)
            
            model_params.update({
                "azure_deployment": deployment,
                "azure_endpoint": endpoint,
                "api_version": api_version,
                "api_key": api_key,
                "max_tokens": max_tokens
            })
        elif self.provider_name == "ollama":
            model_params.update({
                "model": model_name,
                "base_url": settings.OLLAMA_BASE_URL,
                "temperature": temperature
            })

        # 5. Instantiate and return the class with its specific parameters.
        logger.debug(f"Instantiating model '{model_name}' with params: {model_params}")
        return llm_class(**model_params)

    def _get_provider_specific_model_name(self, model_alias: str) -> str:
        """Get the provider-specific model name based on alias and current provider.
        
        This method automatically maps logical aliases to the correct model names
        for each provider, eliminating the need to change config files.
        """
        # Provider-specific model mappings
        provider_models = {
            "groq": {
                "default": "llama3-70b-8192",
                "fast": "llama3-8b-8192",
                "evaluator": "llama3-70b-8192"
            },
            "ollama": {
                "default": "llama3.1:8b",
                "fast": "llama3.1:8b",
                "evaluator": "llama3.1:8b"
            },
            "openai": {
                "default": "gpt-4o-mini",
                "fast": "gpt-4o-mini",
                "evaluator": "gpt-4o"
            },
            "azure_openai": {
                "default": "gpt-4",
                "fast": "gpt-4o-mini",
                "evaluator": "gpt-4"
            }
        }
        
        # Get the model mapping for the current provider
        provider_model_map = provider_models.get(self.provider_name, {})
        
        # Return the provider-specific model name, or fall back to config if not found
        return provider_model_map.get(model_alias, self.model_map.get(model_alias))

# Create a singleton instance for easy import across the application.
# This ensures we only have one instance of the service managing our LLM access.
llm_service = LLMService()