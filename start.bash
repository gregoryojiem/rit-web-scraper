#!/bin/bash

# Check if script is running as root
if [ "$(id -u)" != "0" ]; then
    echo "Requesting administrator privileges..."
    sudo "$0" "$@"
    exit $?
fi

# Get script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# Run main script
python3 main.py