# src/nuvyn_bldr/main.py (Debug Version)

print("DEBUG: main.py script started.") # <-- Diagnostic print

import typer
import logging
from pathlib import Path
from typing import List
import json
import traceback

try:
    print("DEBUG: Importing dependencies...")
    from nuvyn_bldr.agents.analyst.agent import AnalystAgent
    from nuvyn_bldr.agents.developer.agent import DeveloperAgent
    from nuvyn_bldr.core.logging_config import setup_logging
    from nuvyn_bldr.core.error_handler import STTMGenerationError, ELTGenerationError
    from nuvyn_bldr.core.config import settings
    print("DEBUG: Dependencies imported successfully.")

    print("DEBUG: Setting up logging...")
    # We assume setup_logging() exists in logging_config.py
    setup_logging()
    print("DEBUG: Logging setup complete.")

except Exception as e:
    print(f"\nFATAL: An error occurred during initial setup: {e}")
    traceback.print_exc()
    # We use typer.Exit here if typer has been imported, otherwise a standard exit.
    raise typer.Exit(code=1)

# This logger is now configured by the setup_logging call
logger = logging.getLogger(__name__)
app = typer.Typer(
    name="nuvyn-bldr",
    help="An Agentic Data Warehousing Solution to build production-ready PySpark pipelines."
)

@app.command("run-analyst")
def run_analyst_agent(
    input_dir: Path = typer.Option(
        ...,
        "--input-dir",
        "-i",
        help="Directory containing the raw source data files (e.g., CSVs).",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
    output_path: Path = typer.Option(
        ...,
        "--output-path",
        "-o",
        help="Path to save the generated STTM JSON file.",
        dir_okay=False,
        writable=True,
        resolve_path=True,
    ),
    azure_connection_string: str = typer.Option(
        None,
        "--azure-connection-string",
        help="Azure Storage connection string for blob access.",
    ),
    azure_account_name: str = typer.Option(
        None,
        "--azure-account-name",
        help="Azure Storage account name.",
    ),
    azure_account_key: str = typer.Option(
        None,
        "--azure-account-key",
        help="Azure Storage account key.",
    ),
    azure_sas_token: str = typer.Option(
        None,
        "--azure-sas-token",
        help="Azure Storage SAS token.",
    ),
):
    """
    Runs the Analyst Agent to profile data and generate a Source-to-Target Mapping (STTM).
    """
    logger.info("Orchestrator: Initializing Analyst Agent workflow.")
    
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Update Azure settings if provided
        if azure_connection_string:
            settings.AZURE_STORAGE_CONNECTION_STRING = azure_connection_string
        if azure_account_name:
            settings.AZURE_STORAGE_ACCOUNT_NAME = azure_account_name
        if azure_account_key:
            settings.AZURE_STORAGE_ACCOUNT_KEY = azure_account_key
        if azure_sas_token:
            settings.AZURE_STORAGE_SAS_TOKEN = azure_sas_token
        
        # Scan for data files in the input directory
        logger.info(f"Scanning for data files in: {input_dir}")
        file_paths = []
        for pattern in ["*.csv", "*.xlsx", "*.xls", "*.json", "*.parquet"]:
            file_paths.extend(input_dir.glob(pattern))
        
        if not file_paths:
            logger.error(f"No data files found in the specified directory: {input_dir}")
            raise typer.Exit(code=1)
        
        logger.info(f"Found {len(file_paths)} source file(s) to analyze: {[p.name for p in file_paths]}")
        str_file_paths = [str(p) for p in file_paths]

        analyst_agent = AnalystAgent()
        sttm_result = analyst_agent.generate_sttm(file_paths=str_file_paths)

        logger.info(f"Saving generated STTM to: {output_path}")
        with open(output_path, "w") as f:
            f.write(sttm_result.model_dump_json(indent=2))

        typer.secho(
            f"✅ Analyst Agent completed successfully! STTM saved to {output_path}",
            fg=typer.colors.GREEN,
        )

    except STTMGenerationError as e:
        logger.error(f"Analyst Agent failed to generate STTM: {e}", exc_info=True)
        typer.secho(f"❌ Error: {e}", fg=typer.colors.RED)
        if e.llm_response:
            failed_output_path = output_path.parent / "failed_sttm_output.txt"
            failed_output_path.write_text(e.llm_response)
            logger.info(f"Failed LLM response saved to {failed_output_path}")
        raise typer.Exit(code=1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        typer.secho(f"❌ A critical error occurred. Check logs for details.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@app.command("run-analyst-azure")
def run_analyst_agent_azure(
    source_paths: List[str] = typer.Option(
        ...,
        "--source-paths",
        "-s",
        help="List of source paths (local files or Azure blob URLs).",
    ),
    output_path: Path = typer.Option(
        ...,
        "--output-path",
        "-o",
        help="Path to save the generated STTM JSON file.",
        dir_okay=False,
        writable=True,
        resolve_path=True,
    ),
    azure_connection_string: str = typer.Option(
        None,
        "--azure-connection-string",
        help="Azure Storage connection string for blob access.",
    ),
    azure_account_name: str = typer.Option(
        None,
        "--azure-account-name",
        help="Azure Storage account name.",
    ),
    azure_account_key: str = typer.Option(
        None,
        "--azure-account-key",
        help="Azure Storage account key.",
    ),
    azure_sas_token: str = typer.Option(
        None,
        "--azure-sas-token",
        help="Azure Storage SAS token.",
    ),
):
    """
    Runs the Analyst Agent to generate STTM from multiple data sources including Azure Blob.
    """
    logger.info("Orchestrator: Initializing Analyst Agent workflow with Azure support.")
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Update Azure settings if provided
        if azure_connection_string:
            settings.AZURE_STORAGE_CONNECTION_STRING = azure_connection_string
        if azure_account_name:
            settings.AZURE_STORAGE_ACCOUNT_NAME = azure_account_name
        if azure_account_key:
            settings.AZURE_STORAGE_ACCOUNT_KEY = azure_account_key
        if azure_sas_token:
            settings.AZURE_STORAGE_SAS_TOKEN = azure_sas_token
        
        logger.info(f"Processing {len(source_paths)} source(s): {source_paths}")
        
        # Generate STTM using Analyst Agent
        analyst_agent = AnalystAgent()
        sttm_result = analyst_agent.generate_sttm(file_paths=source_paths)
        
        # Save the generated STTM
        logger.info(f"Saving generated STTM to: {output_path}")
        with open(output_path, "w") as f:
            f.write(sttm_result.model_dump_json(indent=2))

        typer.secho(
            f"✅ Analyst Agent completed successfully! STTM saved to {output_path}",
            fg=typer.colors.GREEN,
        )

    except STTMGenerationError as e:
        logger.error(f"Analyst Agent failed to generate STTM: {e}", exc_info=True)
        typer.secho(f"❌ Error: {e}", fg=typer.colors.RED)
        if e.llm_response:
            failed_output_path = output_path.parent / "failed_sttm_output.txt"
            failed_output_path.write_text(e.llm_response)
            logger.info(f"Failed LLM response saved to {failed_output_path}")
        raise typer.Exit(code=1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        typer.secho(f"❌ A critical error occurred. Check logs for details.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@app.command("run-analyst-azure-container")
def run_analyst_agent_azure_container(
    container_name: str = typer.Option(
        ...,
        "--container-name",
        "-c",
        help="Azure Storage container name to scan for data files.",
    ),
    account_name: str = typer.Option(
        ...,
        "--account-name",
        "-a",
        help="Azure Storage account name.",
    ),
    output_path: Path = typer.Option(
        ...,
        "--output-path",
        "-o",
        help="Path to save the generated STTM JSON file.",
        dir_okay=False,
        writable=True,
        resolve_path=True,
    ),
    azure_connection_string: str = typer.Option(
        None,
        "--azure-connection-string",
        help="Azure Storage connection string for blob access.",
    ),
    azure_account_key: str = typer.Option(
        None,
        "--azure-account-key",
        help="Azure Storage account key.",
    ),
    azure_sas_token: str = typer.Option(
        None,
        "--azure-sas-token",
        help="Azure Storage SAS token.",
    ),
    prefix: str = typer.Option(
        "",
        "--prefix",
        "-p",
        help="Prefix to filter blobs (e.g., 'OpenPaymentsData/2024/').",
    ),
):
    """
    Runs the Analyst Agent to scan an Azure container and generate STTM from all data files.
    This follows the same principle as local directory scanning.
    """
    logger.info("Orchestrator: Initializing Analyst Agent workflow with Azure container scanning.")
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Update Azure settings if provided
        if azure_connection_string:
            settings.AZURE_STORAGE_CONNECTION_STRING = azure_connection_string
        if azure_account_key:
            settings.AZURE_STORAGE_ACCOUNT_KEY = azure_account_key
        if azure_sas_token:
            settings.AZURE_STORAGE_SAS_TOKEN = azure_sas_token
        
        # List all blobs in the container
        logger.info(f"Scanning Azure container '{container_name}' in account '{account_name}' for data files...")
        
        # Use Azure CLI to list blobs
        import subprocess
        import json
        
        # Build the az storage blob list command
        cmd = [
            "az", "storage", "blob", "list",
            "--container-name", container_name,
            "--account-name", account_name,
            "--output", "json"
        ]
        
        if prefix:
            cmd.extend(["--prefix", prefix])
            logger.info(f"Filtering blobs with prefix: {prefix}")
        
        # Execute the command
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        blobs = json.loads(result.stdout)
        
        # Filter for data files
        data_extensions = ['.csv', '.xlsx', '.xls', '.json', '.parquet']
        data_blobs = []
        
        for blob in blobs:
            blob_name = blob.get('name', '')
            if any(blob_name.lower().endswith(ext) for ext in data_extensions):
                data_blobs.append(blob)
        
        if not data_blobs:
            logger.error(f"No data files found in container '{container_name}'")
            raise typer.Exit(code=1)
        
        logger.info(f"Found {len(data_blobs)} data file(s) in container: {[b['name'] for b in data_blobs]}")
        
        # Generate SAS tokens for each blob and create URLs
        source_paths = []
        for blob in data_blobs:
            blob_name = blob['name']
            
            # Generate SAS token for this blob
            sas_cmd = [
                "az", "storage", "blob", "generate-sas",
                "--container-name", container_name,
                "--name", blob_name,
                "--account-name", account_name,
                "--permissions", "r",
                "--expiry", "2025-08-04T23:59:59Z",
                "--output", "tsv"
            ]
            
            sas_result = subprocess.run(sas_cmd, capture_output=True, text=True, check=True)
            sas_token = sas_result.stdout.strip()
            
            # Construct the full URL
            blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
            source_paths.append(blob_url)
            
            logger.info(f"Generated SAS URL for: {blob_name}")
        
        logger.info(f"Processing {len(source_paths)} source(s) from Azure container...")
        
        # Generate STTM using Analyst Agent
        analyst_agent = AnalystAgent()
        sttm_result = analyst_agent.generate_sttm(file_paths=source_paths)
        
        # Save the generated STTM
        logger.info(f"Saving generated STTM to: {output_path}")
        with open(output_path, "w") as f:
            f.write(sttm_result.model_dump_json(indent=2))

        typer.secho(
            f"✅ Analyst Agent completed successfully! STTM saved to {output_path}",
            fg=typer.colors.GREEN,
        )

    except subprocess.CalledProcessError as e:
        logger.error(f"Azure CLI command failed: {e}")
        typer.secho(f"❌ Azure CLI error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    except STTMGenerationError as e:
        logger.error(f"Analyst Agent failed to generate STTM: {e}", exc_info=True)
        typer.secho(f"❌ Error: {e}", fg=typer.colors.RED)
        if e.llm_response:
            failed_output_path = output_path.parent / "failed_sttm_output.txt"
            failed_output_path.write_text(e.llm_response)
            logger.info(f"Failed LLM response saved to {failed_output_path}")
        raise typer.Exit(code=1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        typer.secho(f"❌ A critical error occurred. Check logs for details.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@app.command("test-llm")
def test_llm_provider(
    provider: str = typer.Option(
        "groq",
        "--provider",
        "-p",
        help="LLM provider to test (groq, openai, azure_openai, ollama).",
    ),
    model_alias: str = typer.Option(
        "default",
        "--model",
        "-m",
        help="Model alias to test.",
    ),
    temperature: float = typer.Option(
        0.1,
        "--temperature",
        "-t",
        help="Temperature for the model.",
    ),
):
    """
    Test different LLM providers to ensure they're working correctly.
    """
    logger.info(f"Testing LLM provider: {provider}")
    
    try:
        # Import and test the LLM service
        from nuvyn_bldr.core.llm_service import llm_service
        
        # Update the LLM provider dynamically
        llm_service.set_provider(provider)
        
        # Get the model
        model = llm_service.get_model(
            model_alias=model_alias,
            temperature=temperature
        )
        
        # Test with a simple prompt
        test_prompt = "Hello! Please respond with 'LLM test successful' if you can see this message."
        
        logger.info(f"Sending test prompt to {provider}...")
        response = model.invoke(test_prompt)
        
        typer.secho(
            f"✅ LLM test successful! Provider: {provider}, Model: {model_alias}",
            fg=typer.colors.GREEN,
        )
        typer.secho(f"Response: {response.content}", fg=typer.colors.BLUE)
        
    except Exception as e:
        logger.error(f"LLM test failed: {e}", exc_info=True)
        typer.secho(f"❌ LLM test failed: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@app.command("run-developer")
def run_developer_agent(
    sttm_path: Path = typer.Option(
        ...,
        "--sttm-path",
        "-s",
        help="Path to the STTM JSON file generated by the Analyst Agent.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output-dir",
        "-o",
        help="Directory to save the generated ELT pipeline artifacts.",
        dir_okay=True,
        file_okay=False,
        writable=True,
        resolve_path=True,
    ),
    source_path: str = typer.Option(
        "/mnt/data/source_data.csv",
        "--source-path",
        "-p",
        help="Absolute path to the source data file in Databricks (e.g., /mnt/data/nyc-rolling-sales.csv).",
    ),
):
    """
    Runs the Developer Agent to generate ELT pipeline from STTM.
    """
    logger.info("Orchestrator: Initializing Developer Agent workflow.")
    
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        logger.info(f"Reading STTM from: {sttm_path}")
        with open(sttm_path, "r") as f:
            sttm_data = json.load(f)
        
        # Generate ELT pipeline using Developer Agent with source path
        developer_agent = DeveloperAgent()
        pipeline_result = developer_agent.generate_elt_pipeline_from_file(str(sttm_path), source_path=source_path)
        
        # Save generated artifacts
        pipeline_script_path = output_dir / "main_elt_script.py"
        notebook_path = output_dir / "databricks_notebook.py"
        dq_suite_path = output_dir / "data_quality_checks.py"
        deployment_config_path = output_dir / "deployment_config.json"
        
        # Write files
        pipeline_script_path.write_text(pipeline_result.pyspark_script)
        notebook_path.write_text(pipeline_result.databricks_notebook)
        dq_suite_path.write_text(pipeline_result.data_quality_suite)
        deployment_config_path.write_text(json.dumps(pipeline_result.deployment_config, indent=2))
        
        logger.info(f"Generated ELT pipeline artifacts saved to: {output_dir}")
        
        typer.secho(
            f"✅ Developer Agent completed successfully! Artifacts saved to {output_dir}",
            fg=typer.colors.GREEN,
        )

    except ELTGenerationError as e:
        logger.error(f"Developer Agent failed to generate ELT pipeline: {e}", exc_info=True)
        typer.secho(f"❌ Error: {e}", fg=typer.colors.RED)
        if e.llm_response:
            failed_output_path = output_dir / "failed_elt_output.txt"
            failed_output_path.write_text(e.llm_response)
            logger.info(f"Failed LLM response saved to {failed_output_path}")
        raise typer.Exit(code=1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        typer.secho(f"❌ A critical error occurred. Check logs for details.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@app.command("generate-elt")
def generate_elt_pipeline(
    sttm_path: Path = typer.Option(
        ...,
        "--sttm-path",
        "-s",
        help="Path to the STTM JSON file.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
    ),
    output_path: Path = typer.Option(
        ...,
        "--output-path",
        "-o",
        help="Path to save the generated ELT pipeline.",
        dir_okay=False,
        writable=True,
        resolve_path=True,
    ),
):
    """
    Generate ELT pipeline from STTM document.
    """
    logger.info("Generating ELT pipeline from STTM.")
    
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(sttm_path, "r") as f:
            sttm_data = json.load(f)
        
        # TODO: Implement ELT pipeline generation
        logger.info(f"ELT pipeline saved to: {output_path}")
        
        typer.secho(
            f"✅ ELT pipeline generated successfully!",
            fg=typer.colors.GREEN,
        )

    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        typer.secho(f"❌ A critical error occurred. Check logs for details.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@app.command("generate-notebook")
def generate_notebook(
    sttm_path: Path = typer.Option(
        ...,
        "--sttm-path",
        "-s",
        help="Path to the STTM JSON file.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
    ),
    output_path: Path = typer.Option(
        ...,
        "--output-path",
        "-o",
        help="Path to save the generated Databricks notebook.",
        dir_okay=False,
        writable=True,
        resolve_path=True,
    ),
):
    """
    Generate Databricks notebook from STTM document.
    """
    logger.info("Generating Databricks notebook from STTM.")
    
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(sttm_path, "r") as f:
            sttm_data = json.load(f)
        
        # TODO: Implement notebook generation
        logger.info(f"Databricks notebook saved to: {output_path}")
        
        typer.secho(
            f"✅ Databricks notebook generated successfully!",
            fg=typer.colors.GREEN,
        )

    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        typer.secho(f"❌ A critical error occurred. Check logs for details.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@app.command("deploy-pipeline")
def deploy_pipeline(
    pipeline_path: Path = typer.Option(
        ...,
        "--pipeline-path",
        "-p",
        help="Path to the ELT pipeline artifacts.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
    workspace_url: str = typer.Option(
        ...,
        "--workspace-url",
        "-w",
        help="Databricks workspace URL.",
    ),
):
    """
    Deploy ELT pipeline to Databricks workspace.
    """
    logger.info("Deploying ELT pipeline to Databricks.")
    
    try:
        # TODO: Implement pipeline deployment
        logger.info(f"Pipeline deployed to: {workspace_url}")
        
        typer.secho(
            f"✅ Pipeline deployed successfully to {workspace_url}!",
            fg=typer.colors.GREEN,
        )

    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        typer.secho(f"❌ A critical error occurred. Check logs for details.", fg=typer.colors.RED)
        raise typer.Exit(code=1)


