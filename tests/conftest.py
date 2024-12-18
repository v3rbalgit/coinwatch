import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Add shared package to Python path for testing
shared_path = project_root / 'coinwatch' / 'shared'
sys.path.insert(0, str(shared_path))

# Add any global test fixtures here
