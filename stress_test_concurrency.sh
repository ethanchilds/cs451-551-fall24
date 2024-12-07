#!/bin/bash

echo "===================================ASSIGNMENT 3 TESTS================================================="

# Default values
arg1=1000
arg2=10
arg3=100000
arg4=8
arg5=3562901

# Parse named arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --number_of_records) arg1="$2"; shift ;;
        --number_of_transactions) arg2="$2"; shift ;;
        --number_of_queries) arg3="$2"; shift ;;
        --num_threads) arg4="$2"; shift ;;
        --seed) arg5="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

FOLDER="./CS451"
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

# Run the first Python file
echo "Running stress_test_concurrency_setup.py"
python3 stress_test_concurrency_setup.py $arg1 $arg2 $arg3 $arg4 $arg5

# Run the second Python file
echo "Running stress_test_concurrency.py"
python3 stress_test_concurrency.py $arg1 $arg2 $arg3 $arg4 $arg5


# Check if the folder exists, then delete it
if [ -d "$FOLDER" ]; then
    rm -rf "$FOLDER"
    echo "Folder '$FOLDER' has been deleted."
else
    echo "Folder '$FOLDER' does not exist."
fi

echo "===================================================================================="