# fd

## Location
src/interfaces/ecpg/test/expected/preproc-init.c: 105 - 110

## Overview
A static test function used in ECPG (Embedded SQL in C) test cases that demonstrates multi-parameter function calls with mixed data types.

## Definition


## Detailed Description
The `fd` function is a test function that takes both a string pointer and an integer parameter. It prints both parameters to stdout and returns the product of the first character of the string (as an ASCII value) multiplied by the integer parameter. This function is part of the ECPG test suite and is used to verify that multi-parameter function calls with different data types work correctly in the ECPG preprocessor and runtime environment.

## Parameters / Member Variables
- `x`: A constant character pointer (string) whose first character is used in the calculation
- `i`: An integer parameter that is multiplied with the first character's ASCII value

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is a static function with internal linkage, only visible within its compilation unit
- Demonstrates multi-parameter function calls in ECPG test scenarios
- Returns the ASCII value of the first character multiplied by the integer parameter
- Part of the ECPG test infrastructure for validating embedded SQL functionality
- Uses const-correctness for the string parameter
- Unlike other test functions in the same file, this function doesn't appear to be directly called by main() based on the reference analysis