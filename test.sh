#!/bin/bash

# Check if the folder exists, then delete it

echo "===================================ASSIGNMENT 1 TESTS================================================="
FOLDER="./TEMP"
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

# Run the first Python file
echo "Running m1_tester.py"
python3 m1_tester.py


# Check if the folder exists, then delete it
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

echo "===================================================================================="


FOLDER="./TEMP"
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

# Run the first Python file
echo "Running exam_tester_m1.py"
python3 exam_tester_m1.py


# Check if the folder exists, then delete it
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

echo "===================================ASSIGNMENT 2 TESTS================================================="
FOLDER="./CS451"
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

# Run the first Python file
echo "Running exam_tester_m2_part1.py"
python3 exam_tester_m2_part1.py

# Run the second Python file
echo "Running exam_tester_m2_part2.py"
python3 exam_tester_m2_part2.py


# Check if the folder exists, then delete it
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi


echo "===================================================================================="
FOLDER="./CS451"
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi
echo "Running m2_tester_part1.py"
python3 m2_tester_part1.py

echo "Running m2_tester_part2.py"
python3 m2_tester_part2.py

# Define the folder to be deleted
FOLDER="./CS451"

# Check if the folder exists, then delete it
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

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
echo "Running exam_tester_m3_part2.py"
python3 exam_tester_m3_part2.py


# Check if the folder exists, then delete it
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

echo "===================================================================================="

echo "===================================TEST.PY================================================="
# Run the first Python file
echo "Running unit test from test.py"
python3 test.py

echo "===================================MAIN BENCHMARK================================================="
FOLDER="./TEMP"
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

# Run the first Python file
echo "Running __main__.py"
python3 __main__.py


# Check if the folder exists, then delete it
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi