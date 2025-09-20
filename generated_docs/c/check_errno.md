# check_errno

## Location
[src/interfaces/ecpg/test/expected/pgtypeslib-num_test2.c:265-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/pgtypeslib-num_test2.c#L265-L290)

## Overview
A diagnostic utility function that examines and reports the current value of the global errno variable, specifically designed to handle PostgreSQL ECPG and pgtypes numeric error conditions.

## Definition

```c
static void
check_errno(void)
```
## Detailed Description
The  function serves as a diagnostic tool that examines the current state of the global  variable and prints a human-readable description of the error condition. It is specifically designed to handle error codes related to PostgreSQL's ECPG (Embedded SQL in C) and pgtypes numeric operations.

The function uses a switch statement to categorize and display different types of numeric errors including overflow, underflow, bad numeric format, and division by zero conditions. For unrecognized errno values, it provides both the numeric error code and the standard library error description via .

This function is primarily used in PostgreSQL's ECPG test suite to provide clear error reporting and validation of error handling mechanisms in numeric operations.

## Parameters / Member Variables
- None (void function that operates on the global errno variable)

## Dependencies
- Functions called/Symbols referenced:
  - printf (multiple calls for different error conditions)
  - strerror (for unknown errno values)
  - ECPG_INFORMIX_NUM_OVERFLOW (error constant)
  - ECPG_INFORMIX_NUM_UNDERFLOW (error constant)
  - PGTYPES_NUM_OVERFLOW (error constant)
  - PGTYPES_NUM_UNDERFLOW (error constant)
  - PGTYPES_NUM_BAD_NUMERIC (error constant)
  - PGTYPES_NUM_DIVIDE_ZERO (error constant)
- Called from (representative examples):
  - [print_double](../p/print_double.md) (in compat_informix-dec_test.c)
  - [main](../m/main.md) (multiple calls in various test files)
  - [ECPGdebug](../E/ECPGdebug.md) (in pgtypeslib-dt_test.c)

## Notes and Other Information
- This function is marked as , limiting its scope to the compilation unit where it's defined
- The function is extensively used throughout PostgreSQL's ECPG test suite for error validation
- It handles both ECPG-specific Informix compatibility errors and general pgtypes numeric errors
- The function provides consistent error reporting format across different test scenarios
- Error messages are formatted with trailing dashes to allow for additional context in test output
- For unknown errno values, it provides both the numeric code and the system error description