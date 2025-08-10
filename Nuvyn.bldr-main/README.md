# Nuvyn.bldr: Agentic Data Warehousing Solution 🤖

## 🔭 Vision

> To provide a turn-key, agentic data warehousing solution that automates the most complex aspects of data engineering, from raw data analysis to production-ready data pipelines, all powered by a self-improving, AI-driven system.

-----

## 📝 Overview

**Nuvyn.bldr** is a SaaS application designed to be listed on the Databricks Marketplace. It provides a powerful, automated data engineering solution that connects securely to a client's Databricks environment.

A user provides Nuvyn.bldr with access to their raw data sources (e.g., CSV files in a cloud storage bucket). From its own cloud-native environment, our system then remotely orchestrates tasks within the client's Databricks workspace to:

1.  **Analyze & Profile** the data to understand its structure, content, and relationships.
2.  **Design a Star Schema** by identifying potential fact and dimension tables.
3.  **Generate a Source-to-Target Mapping (STTM)** document.
4.  **Write and deploy** a production-ready PySpark pipeline as a Databricks notebook into the client's workspace.
5.  **Validate** the generated data against defined quality rules using the client's compute resources.
6.  **Continuously Improve** its performance by learning from every run.

-----

## 🏗️ Core Architecture

The platform is built on a sophisticated, agent-based architecture designed for a multi-tenant SaaS deployment.

### Deployment Model

The entire **Nuvyn.bldr** agent fleet is designed to be deployed as a containerized application on a cloud platform like Azure Container Apps. It interacts with client environments securely and remotely.

### Client Interaction

The application uses the **Databricks REST API** (authenticating via a client-provided Service Principal) to manage all resources and orchestrate jobs within the client's Databricks workspace.

### Agent Fleet

We have a team of specialized AI agents:

  * **Analyst Agent**: Profiles data and designs the target data warehouse schema (STTM).
  * **Developer Agent**: Writes the PySpark code and manages deployment to the client's Databricks workspace.
  * **Tester Agent**: Independently validates the STTM logic and the data produced by the pipeline.

### Supervisor & Orchestration

The entire workflow is managed as a cyclical graph using **LangGraph**. The **Supervisor Agent** orchestrates the process, manages a central state, and handles a refinement loop, sending tasks back to the appropriate agent if validation fails.

### The Learning Loop

The system is designed to be self-improving.

  * An **Evaluator Agent** assesses the quality of every completed run.
  * A **Knowledge Base** (vector store) is updated with "golden examples" of successful runs and "failure-correction" cases from failed runs.
  * Before acting, the **Analyst** and **Developer** agents query this Knowledge Base using Retrieval-Augmented Generation (RAG) to improve the quality of their output.

-----

## 📂 Repository Structure Walkthrough

This repository follows the standard `src-layout` for modern Python applications. This ensures clean imports and makes the project easily installable.

```
nuvyn-bldr/
│
├── data/
│   └── ...                 # Sample datasets for local testing.
│
├── docs/
│   └── ...                 # Project documentation (architecture, schemas).
│
├── notebooks/
│   └── ...                 # Jupyter notebooks for developer exploration & testing.
│
├── output/
│   └── ...                 # (Git Ignored) Default location for runtime artifacts during local runs.
│
├── src/
│   └── nuvyn_bldr/
│       ├── __init__.py
│       │
│       ├── agents/         # Core logic for each individual agent.
│       │   ├── analyst/
│       │   ├── developer/
│       │   ├── tester/
│       │   ├── evaluator/
│       │   └── supervisor/
│       │
│       ├── core/           # Shared, non-agent-specific business logic.
│       │   ├── databricks_service.py
│       │   ├── knowledge_base.py
│       │   └── profiling.py
│       │
│       ├── prompts/        # Centralized library of all prompt templates.
│       │   ├── analyst_prompts.py
│       │   └── ...
│       │
│       ├── schemas/        # Pydantic models defining our data contracts (STTM, etc.).
│       │   ├── sttm.py
│       │   └── ...
│       │
│       ├── config.py       # Application configuration loader.
│       └── main.py         # Main entry point to run the Supervisor.
│
├── tests/
│   └── ...                 # Unit and integration tests.
│
├── .env                    # (Git Ignored) Stores secrets and configuration.
├── .gitignore
├── pyproject.toml          # Project metadata and dependencies.
└── README.md               # This file.
```

### Key Directory Explanations

  * `data/`: Contains sample datasets used for local development and testing. This directory is not part of the deployed application.
  * `docs/`: All project-level documentation resides here, including detailed architecture diagrams, schema definitions, and other planning documents.
  * `notebooks/`: A scratchpad for developers. Use this for data exploration, prototyping new ideas, or one-off agent tests.
  * `output/`: This directory is ignored by Git. When running the application locally, any generated files will be saved here for easy inspection.
  * `src/nuvyn_bldr/`: This is the main application package.
      * `agents/`: Each agent is a self-contained module responsible for its specific role.
      * `core/`: Reusable business logic shared across the application (e.g., interacting with the Databricks API).
      * `prompts/`: All LLM prompt templates are stored here, separated by agent for easy management.
      * `schemas/`: Contains the Pydantic models that define the "data contracts" between agents.
  * `tests/`: Contains all unit and integration tests. The structure of this directory should mirror the `src/nuvyn_bldr/` directory.

-----

## 🚀 Getting Started

### Prerequisites

  * Python 3.10+
  * An active Databricks workspace and API access (via Service Principal).
  * Access to an LLM API (e.g., Groq).

### Setup Instructions

1.  **Clone the repository:**
    ```sh
    git clone <repository_url>
    cd nuvyn-bldr
    ```
2.  **Create and activate a virtual environment:**
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```
3.  **Install dependencies:**
    The project uses `pyproject.toml` to manage dependencies. The `-e .` flag installs the package in "editable" mode.
    ```sh
    pip install -e .
    ```
4.  **Configure your environment:**
    Create a `.env` file in the project root by copying the example file.
    ```sh
    cp .env.example .env
    ```
    Now, edit the `.env` file and fill in your specific credentials:
    ```ini
    # .env
    GROQ_API_KEY="your_groq_api_key"
    DATABRICKS_HOST="https://your-workspace.databricks.net"
    DATABRICKS_CLIENT_ID="your_service_principal_client_id"
    DATABRICKS_CLIENT_SECRET="your_service_principal_client_secret"
    DATABRICKS_TENANT_ID="your_azure_tenant_id"
    ```

-----

## 🏃 How to Run

### Running the Full Application

The main entry point orchestrates the entire process via the Supervisor Agent.

```sh
python src/nuvyn_bldr/main.py --source-dir ./data/ecommerce
```

### Running Tests

To ensure the integrity of the codebase, run the test suite using pytest.

```sh
pytest
```

-----

## ✍️ Coding Guidelines

  * **Formatting**: All code is formatted with **Black**.
  * **Imports**: All imports are sorted with **isort**.
  * **Type Hinting**: All functions and methods must be fully type-hinted.
  * **Logging**: Use the standard `logging` module for all application output.
  * **Docstrings**: All public modules, classes, and functions must have Google-style docstrings.

-----

## 🌳 Branching Strategy

This project follows a simplified GitFlow model that emphasizes agility while ensuring stability for production releases.

### Core Branches

  * **main**: Represents the stable, production-ready code. Direct commits are forbidden.
  * **develop**: The primary integration branch for all new features. This is our "nightly build."

### Supporting Branches

  * `feature/<feature-name>`: For developing new features. Branches from `develop` and merges back into `develop`.
  * `release/<version-number>`: To prepare a new production release. This acts as our temporary QA/UAT branch. Branches from `develop` and merges into both `main` and `develop`.
  * `hotfix/<issue-name>`: For critical production bug fixes. Branches from `main` and merges back into both `main` and `develop`.

### Visualizing the Code Flow

```
main      ------------------------------------o----------o-------------------> (v1.0.0)  (v1.0.1)
           \                                 / \        /
release     \-------------------------------o---'      /
             \                             /          /
develop   ----o-------o---------o---------o----------o---------o------------>
               \     /         /         /          /
feature         o---'         /         /          /
                             /         /          /
feature                     o---------'          /
                                                /
hotfix                                         o----'
```

The standard lifecycle is: `feature` -\> `develop` -\> `release` -\> `main`. The `release` branch is deployed to a dedicated SIT/UAT environment for final testing before being merged into `main` for the production release.