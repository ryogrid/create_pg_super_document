# dosqlprint

## Location
src/interfaces/ecpg/test/expected/compat_informix-test_informix.c: 28 - 31

## Overview
A static utility function used in ECPG (Embedded SQL in C) compatibility testing to print SQL error messages to standard output.

## Definition


## Detailed Description
The dosqlprint function is a simple error reporting utility used within the ECPG test framework for Informix compatibility. It prints formatted error messages to stdout by accessing the global sqlca (SQL Communications Area) structure. The function specifically outputs the error message stored in sqlca.sqlerrm.sqlerrmc, which contains the detailed error text from the most recent SQL operation.

This function is part of the expected output generation for ECPG compatibility tests, ensuring that error handling behaves consistently with Informix-style error reporting patterns.

## Parameters / Member Variables
This function takes no parameters and returns void.

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard C library function)
  - sqlca (global SQL Communications Area structure)
- Called from (representative examples):
  - [main](../m/main.md) (called 21 times throughout the test program)
  - [openit](../o/openit.md) (called once during database connection testing)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only visible within the compilation unit
- Used exclusively for testing ECPG's Informix compatibility layer
- The function name follows Informix naming conventions ("doSQLprint" style)
- Part of the expected test output framework, not production database code
- Accesses the global sqlca structure which is standard in embedded SQL programming