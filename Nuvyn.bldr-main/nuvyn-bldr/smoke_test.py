print("--- Smoke Test Starting ---")
try:
    # This is the most important line.
    # We are trying to see if Python can find your 'main' module.
    from src.nuvyn_bldr import main
    print("SUCCESS: The 'main' module was found and imported.")
    
except ImportError as e:
    print(f"FAILURE: Could not import the module.")
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

print("--- Smoke Test Finished ---")