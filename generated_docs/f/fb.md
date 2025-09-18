# fb

## Location
[src/interfaces/ecpg/test/expected/preproc-init.c:92-98](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-init.c#L92-L98)

## Overview
A static test function used in ECPG (Embedded SQL in C) test cases that demonstrates function parameter passing and return value handling.

## Definition


## Detailed Description
The `fb` function is a test function that takes an integer parameter and returns it unchanged. It prints a debug message to stdout showing the input parameter value. This function is part of the ECPG test suite and is used to verify that parameter passing and function calls work correctly in the ECPG preprocessor and runtime environment.

## Parameters / Member Variables
- `x`: An integer parameter that is printed and returned unchanged

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function)
- Called from (representative examples):
  - [main](../m/main.md) (in src/interfaces/ecpg/test/expected/preproc-init.c:165)
  - [main](../m/main.md) (in src/interfaces/ecpg/test/expected/preproc-init.c:221)

## Notes and Other Information
- This is a static function with internal linkage, only visible within its compilation unit
- Demonstrates parameter passing in ECPG test scenarios
- Returns the same integer value that was passed as input
- Part of the ECPG test infrastructure for validating embedded SQL functionality
- The function prints its parameter for debugging/verification purposes