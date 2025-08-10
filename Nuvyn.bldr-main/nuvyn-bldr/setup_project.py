# setup_project.py
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_directory_structure():
    """
    Creates the complete directory structure for the Nuvyn.bldr project.
    """
    logging.info("Starting project scaffolding for Nuvyn.bldr...")

    # --- List of all directories to be created ---
    # The structure includes the src-layout and all sub-packages.
    dirs_to_create = [
        "docs",
        "notebooks",
        "output",
        "src/nuvyn_bldr/agents/analyst",
        "src/nuvyn_bldr/agents/developer",
        "src/nuvyn_bldr/agents/tester",
        "src/nuvyn_bldr/agents/evaluator",
        "src/nuvyn_bldr/agents/supervisor",
        "src/nuvyn_bldr/prompts",
        "src/nuvyn_bldr/schemas",
        "tests"
    ]

    # --- List of placeholder files to ensure directories are tracked by Git ---
    # These files will be created inside the specified directories.
    gitkeep_files = [
        "docs/.gitkeep",
        "notebooks/.gitkeep",
        "output/.gitkeep",
        "src/nuvyn_bldr/agents/analyst/.gitkeep",
        "src/nuvyn_bldr/agents/developer/.gitkeep",
        "src/nuvyn_bldr/agents/tester/.gitkeep",
        "src/nuvyn_bldr/agents/evaluator/.gitkeep",
        "src/nuvyn_bldr/agents/supervisor/.gitkeep",
        "src/nuvyn_bldr/prompts/.gitkeep",
        "src/nuvyn_bldr/schemas/.gitkeep",
        "tests/.gitkeep"
    ]
    
    # --- List of essential empty __init__.py files to define packages ---
    init_files = [
        "src/nuvyn_bldr/__init__.py",
        "src/nuvyn_bldr/agents/__init__.py",
        "src/nuvyn_bldr/core/__init__.py",
        "tests/__init__.py"
    ]

    # Create directories
    for path in dirs_to_create:
        try:
            os.makedirs(path, exist_ok=True)
            logging.info(f"Created directory: {path}")
        except OSError as e:
            logging.error(f"Error creating directory {path}: {e}")

    # Create .gitkeep files
    for file_path in gitkeep_files:
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                pass  # Create an empty file
            logging.info(f"Created placeholder file: {file_path}")

    # Create __init__.py files
    for file_path in init_files:
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                pass # Create an empty file
            logging.info(f"Created package file: {file_path}")

    logging.info("Project scaffolding complete!")

if __name__ == "__main__":
    create_directory_structure()