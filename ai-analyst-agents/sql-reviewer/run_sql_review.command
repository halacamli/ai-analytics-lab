#!/bin/bash

# Move to script directory
cd "$(dirname "$0")/cli"

echo "Activating environment..."
source .venv/bin/activate

echo ""
echo "Drop your SQL file into this window and press ENTER:"
read SQL_FILE

python reviewer.py --input "$SQL_FILE"

echo ""
echo "✅ Review completed!"
read -p "Press ENTER to close..."
