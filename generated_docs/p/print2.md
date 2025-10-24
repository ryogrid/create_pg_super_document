# print2

## Location
[src/interfaces/ecpg/test/expected/preproc-whenever.c:33-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-whenever.c#L33-L38)

## Overview
The print2 function is a static error handling utility in ECPG test code that prints an error message and displays SQL diagnostic information.

## Definition
```c
static void print2(void)
```

## Detailed Description
The print2 function is a simple error handling routine specifically designed for ECPG (Embedded SQL in C) testing. It outputs a fixed error message "Found another error\n" to stderr and then calls sqlprint() to display additional SQL diagnostic information. This function serves as a standardized way to report secondary or additional errors encountered during ECPG test execution.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard error output)
  - [sqlprint](../s/sqlprint.md) (ECPG function for SQL diagnostics)
- Called from (representative examples):
  - [main](../m/main.md) (in the same test file at lines 162 and 171)

## Notes and Other Information
- This function is specific to ECPG test infrastructure
- Located in src/interfaces/ecpg/test/expected/preproc-whenever.c:33-38
- Static function scope limits its visibility to the containing file
- Part of PostgreSQL's ECPG testing framework
- Used for error reporting in "whenever" preprocessor test scenarios
- Works in conjunction with sqlprint() to provide comprehensive error information

## Simplified Source

```c
static void print2(void) {
    // Print error message to stderr
    fprintf(stderr, "Found another error\n");

    // Display SQL diagnostic information
    sqlprint();
}
```