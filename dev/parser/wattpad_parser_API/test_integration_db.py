
import sys
import os
import logging

# Setup logging to console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_integration")

# Add project root to sys.path
sys.path.append(os.getcwd())

print("--- Starting Integration Test ---")

try:
    print("1. Importing modules from wattpad_parser package...")
    from wattpad_parser.database import DatabaseManager
    from wattpad_parser.parser import WattpadParser
    
    print("2. Initializing DatabaseManager...")
    db_manager = DatabaseManager()
    
    print("3. Connecting to Database (Mock)...")
    db_manager.connect()
    print("   SUCCESS: Database connected.")

    print("4. Initializing WattpadParser with DB...")
    # Initialize with minimal params just to check constructor
    parser = WattpadParser(
        db_manager=db_manager,
        year=2024,
        languages=['ru'],
        headless=True
    )
    print("   SUCCESS: Parser initialized.")
    
    print("5. Disconnecting Database...")
    db_manager.disconnect()
    print("   SUCCESS: Database disconnected.")
    
    print("\nALL TESTS PASSED: No 'AttributeError' or import errors found.")

except Exception as e:
    print(f"\nTEST FAILED: {e}")
    # Print full traceback for debugging
    import traceback
    traceback.print_exc()
    sys.exit(1)
