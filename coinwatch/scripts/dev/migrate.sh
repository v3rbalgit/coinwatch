#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Get the project root directory (two levels up from scripts/dev)
PROJECT_ROOT="$SCRIPT_DIR/../.."

# Source the environment variables
source "$PROJECT_ROOT/.env"

# Set PYTHONPATH and run alembic command
PYTHONPATH=$PYTHONPATH:$PROJECT_ROOT alembic -c "$PROJECT_ROOT/shared/database/alembic.ini" "$@"