# printResult

## Location
[src/interfaces/ecpg/test/expected/sql-declare.c:607-618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/sql-declare.c#L607-L618)

## Overview
The printResult function displays the results of ECPG test cases by printing the contents of global result arrays f1, f2, and f3 with formatted output.

## Definition

```c
}

void printResult(char *tc_name, int loop)
```
## Detailed Description
The printResult function serves as a utility for displaying test results in the ECPG test framework. It takes a test case name and the number of result rows to display, then formats and prints the contents of the global arrays f1, f2, and f3 that contain query results. The function provides a standardized way to output test results with clear formatting, including a header with the test case name and structured display of the data values.

The function prints each row of results showing the values from all three arrays (f1, f2, f3) in a consistent format, making it easy to verify test outcomes and debug issues. It adds visual separation with asterisks around the test case name and includes blank lines for readability.

## Parameters / Member Variables
- `*tc_name`: A string containing the name of the test case being printed (can be NULL)
- `loop`: The number of result rows to display from the global arrays
## Dependencies
- Functions called/Symbols referenced:
  - printf (C standard library function for formatted output)
  - References global arrays: f1, f2, f3 (implicitly accessed for printing values)
- Called from:
  - [execute_test](../e/execute_test.md) (multiple times at src/interfaces/ecpg/test/expected/sql-declare.c:297, 366, 403, 471)

## Notes and Other Information
- This function is part of the ECPG test suite infrastructure for result validation and debugging
- Handles NULL tc_name gracefully by checking the pointer before printing the header
- The formatting assumes f1 and f2 are integer arrays and f3 is a character array
- Provides consistent output formatting across all test cases in the suite
- Uses a simple loop to iterate through the specified number of result rows
- Always adds a trailing newline for visual separation between test outputs
- The function assumes the global arrays f1, f2, f3 have been populated by previous database operations
- Located in src/interfaces/ecpg/test/expected/sql-declare.c:607-618
- Called once for each of the four main test cases in execute_test function

## Simplified Source

```c
void printResult(char *tc_name, int loop) {
    int i;

    // Print test case header if name provided
    if (tc_name)
        printf("****%s test results:****\n", tc_name);

    // Print all result rows in formatted output
    for (i = 0; i < loop; i++)
        printf("f1=%d, f2=%d, f3=%s\n", f1[i], f2[i], f3[i]);

    printf("\n");
}
```

This function displays ECPG test results by printing a header with the test case name (if provided) followed by formatted output of result rows from global arrays f1, f2, and f3. It's a simple utility for consistent test output formatting.