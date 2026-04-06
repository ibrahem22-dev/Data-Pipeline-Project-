import sys
import os

# Add the src directory to the Python path so that tests can import modules directly.
# This is necessary because the tests are in a subdirectory of src.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
