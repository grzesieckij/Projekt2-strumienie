#!/bin/bash

# Directory where files will be moved
TARGET_DIR="crimes-in-chicago_result"

# Create the target directory if it does not exist
if [ ! -d "$TARGET_DIR" ]; then
    mkdir -p "$TARGET_DIR"
fi

# Move files matching the pattern to the target directory
for file in part-*-*.csv; do
    if [ -e "$file" ]; then
        mv "$file" "$TARGET_DIR"
        echo "Moved $file to $TARGET_DIR"
    else
        echo "No files matching the pattern were found."
    fi
done

