import os
import shutil
import argparse



M1_TESTS = ['m1_tester.py', 'exam_tester_m1.py']
M2_TESTS = ['m2_tester_part1.py', 'm2_tester_part2.py', 'exam_tester_m2_part1.py', 'exam_tester_m2_part2.py']
M3_TESTS = ['m3_tester_part_1.py', 'm3_tester_part_2.py', 'exam_tester_m3_part1.py', 'exam_tester_m3_part2.py', 'exam_tester_m3_part1.py', 'exam_tester_m3_part2_correct.py']

def m1_tests():
    print('EXECUTING M1 TESTS')
    print('-----------------------------')

    for test_file in M1_TESTS:
        print(f'\nRUNNING {test_file}:')
        try:
            with open(test_file) as file:
                exec(file.read(), globals())
        except:
            print(f"An error occurred with: {test_file}")

        db_path = './TEMP'
        if (os.path.exists(db_path)):
            shutil.rmtree(db_path, ignore_errors=True)

    print('\n')


def m2_tests():
    print('EXECUTING M2 TESTS')
    print('-----------------------------')

    for test_file in M2_TESTS:
        print(f'\nRUNNING {test_file}:')
        try:
            with open(test_file) as file:
                exec(file.read(), globals())
        except:
            print(f"An error occurred with: {test_file}")

        if test_file.endswith("_part2.py"):
            db_path = 'CS451'
            if (os.path.exists(db_path)):
                shutil.rmtree(db_path, ignore_errors=True)

    print('\n')

def m3_tests():

    print('EXECUTING M3 TESTS')
    print('-----------------------------')

    for test_file in M3_TESTS:
        print(f'\nRUNNING {test_file}:')
        try:
            with open(test_file) as file:
                exec(file.read(), globals())
        except Exception as e:
            print(e)
            print(f"An error occurred with: {test_file}")

        if test_file.endswith("_part2.py") or test_file.endswith("_part_2.py") or test_file.endswith("_part2_correct.py"):
            db_path = 'CS451'
            if (os.path.exists(db_path)):
                shutil.rmtree(db_path, ignore_errors=True)

    print('\n')


TEST_MAP = {
    "M1": m1_tests,
    "M2": m2_tests,
    "M3": m3_tests
}


def main():

    parser = argparse.ArgumentParser(description="Run tests")
    parser.add_argument(
        "-t", "--test",
        help="Specify a project to test"
    )
    parser.add_argument(
        "-lt", "--list-tests",
        action="store_true",  # Flag that doesn't require a value
        help="List all available test classes and exit."
    )

    args = parser.parse_args()

    if args.list_tests:
        print("Available tests:")
        for test_name in TEST_MAP:
            print(f"- {test_name}")
        return
    

    if args.test:
        try:
            TEST_MAP[args.test]()
        except KeyError:
            print(f"Test class '{args.test}' not found.")
            return
    else:
        for test in TEST_MAP:
            TEST_MAP[test]()

    print('COMPLETE')
        
if __name__ == "__main__":
    main()