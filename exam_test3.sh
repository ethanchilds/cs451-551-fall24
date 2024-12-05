#!/bin/bash

# Check if the folder exists, then delete it

echo "===================================ASSIGNMENT 3 TESTS================================================="
FOLDER="./CS451"
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

# Run the first Python file
echo "Running exam_tester_m3_part1.py"
python3 exam_tester_m3_part1.py

# Run the second Python file
echo "Running exam_tester_m3_part2_correct.py"
python3 exam_tester_m3_part2_correct.py


# Check if the folder exists, then delete it
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

echo "===================================================================================="
