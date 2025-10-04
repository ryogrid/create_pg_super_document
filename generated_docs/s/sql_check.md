# sql_check

## Location
[src/interfaces/ecpg/test/expected/compat_informix-test_informix2.c:100-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/compat_informix-test_informix2.c#L100-L135)

## Overview
A static error handling function in PostgreSQL's ECPG (Embedded SQL in C) test framework that checks the SQLCODE status and provides standardized error reporting and recovery.

## Definition

```c
/* Check SQLCODE, and produce a "standard error" if it's wrong! */
static void sql_check(const char *fn, const char *caller, int ignore)
```
## Detailed Description
The  function serves as a centralized error handling mechanism for ECPG test cases. It examines the global SQLCODE variable to determine if an SQL operation has failed, and if so, provides detailed error reporting including the function name, caller context, and error message from sqlca. When an error is detected (SQLCODE != 0 and SQLCODE != ignore), the function attempts automatic rollback recovery and terminates the program if the error is not ignorable.

This function is part of the ECPG compatibility layer for Informix, providing a standardized way to handle SQL errors in embedded SQL applications. It ensures consistent error reporting format across test cases and implements defensive programming practices by attempting transaction rollback on errors.

## Parameters / Member Variables
- `*fn`: The name of the function where the SQL operation was performed (for error context)
- `*caller`: Description of the SQL operation that was being performed (for error context)
- `ignore`: An error code that should be ignored (function returns early if SQLCODE matches this value)
## Dependencies
- Functions called/Symbols referenced:
  - SQLCODE (global variable)
  - sqlca.sqlerrm.sqlerrmc (global error message structure)
  - sprintf
  - fprintf  
  - printf
  - [ECPGtrans](../E/ECPGtrans.md) (for rollback operation)
  - exit
- Called from (representative examples):
  - [main](../m/main.md) (multiple locations in test_informix2.c at lines 178, 193, 201, 211, 225, 244, 259, 274)

## Notes and Other Information
- This is a static function local to the test file, indicating it's designed for internal error handling within specific test scenarios
- The function implements a "fail-fast" approach by calling exit(1) on unhandled errors
- Automatic rollback attempt demonstrates defensive programming for transaction integrity
- Part of PostgreSQL's ECPG test suite, specifically for Informix compatibility testing
- Error messages are output to both stderr and stdout for comprehensive logging
- The ignore parameter allows certain expected error conditions to be bypassed during testing

## Simplified Source

```c
static void sql_check(const char *fn, const char *caller, int ignore) {
    char errorstring[255];

    // Return early if SQLCODE matches the ignore value
    if (SQLCODE == ignore)
        return;

    // Handle SQL errors
    if (SQLCODE != 0) {
        // Format and display error message
        sprintf(errorstring, "**SQL error %ld doing '%s' in function '%s'. [%s]",
                SQLCODE, caller, fn, sqlca.sqlerrm.sqlerrmc);
        fprintf(stderr, "%s", errorstring);
        printf("%s\n", errorstring);

        // Attempt rollback
        ECPGtrans(__LINE__, NULL, "rollback");

        // Report rollback result
        if (SQLCODE == 0) {
            sprintf(errorstring, "Rollback successful.\n");
        } else {
            sprintf(errorstring, "Rollback failed with code %ld.\n", SQLCODE);
        }

        fprintf(stderr, "%s", errorstring);
        printf("%s\n", errorstring);

        // Exit on error
        exit(1);
    }
}
```