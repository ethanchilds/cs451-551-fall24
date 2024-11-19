import subprocess

def count_lines_of_code(included_paths=None):
    """
    Counts the total number of lines of code in a Git repository, filtered by specific folders or files.

    Args:
        included_paths (list, optional): A list of folders or file paths to include in the count. 
                                         If None, all files in the repository are included.
    """
    # Get list of files tracked by the Git repository
    result = subprocess.run(['git', 'ls-files'], capture_output=True, text=True)
    files = result.stdout.splitlines()

    # If included_paths is specified, filter files by matching substrings
    if included_paths:
        files = [file for file in files if any(file.startswith(path) for path in included_paths)]

    total_lines = 0
    for file in files:
        try:
            # Open the file in read mode, ignoring any encoding errors
            with open(file, 'r', errors='ignore') as f:
                # Concise way to count lines using a generator expression
                total_lines += sum(1 for _ in f)
        except (OSError, IOError) as e:
            print(f"Could not read file {file}: {e}")

    # Print the total number of lines found
    print(f"Total lines of code: {total_lines}")

if __name__ == "__main__":
    # Specify folders or files you want to include (relative paths from the repository root)
    included_paths = [
        "data_structures/",
        "lstore/",
        "tests/",
        "utilities/",
        "config.py",
        "errors.py",
        "mergeTest.py",
        "mergeThreadTest.py",
        "run_all_unit_tests.py",
        "test_data_structures.py",
        "test.py",
        "test.sh"
    ]
    count_lines_of_code(included_paths)