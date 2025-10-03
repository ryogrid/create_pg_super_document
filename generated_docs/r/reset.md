# reset

## Location
[src/interfaces/ecpg/test/expected/sql-declare.c:600-606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/sql-declare.c#L600-L606)

## Overview
The reset function clears all output variables used in ECPG test cases by zeroing out the global arrays f1, f2, and f3.

## Definition

```c
*/
void reset()
```
## Detailed Description
The reset function serves as a utility function in the ECPG test framework that initializes/clears the global output variables used for storing query results. It uses memset to zero out three global arrays (f1, f2, f3) that are used to store integer and character data retrieved from database queries during test execution. This function ensures a clean state before running each test case, preventing contamination from previous test results.

The function is typically called at the beginning of each test case within the execute_test function to ensure that the result arrays start from a known clean state.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - memset (C standard library function to set memory to specified value)
  - References global arrays: f1, f2, f3 (implicitly through memset operations)
- Called from:
  - [execute_test](../e/execute_test.md) (multiple times at src/interfaces/ecpg/test/expected/sql-declare.c:235, 304, 372, 409)
  - Various other PostgreSQL internal functions (shown in extensive reference list)

## Notes and Other Information
- This is a utility function specifically designed for the ECPG test suite
- Clears three global arrays: f1 and f2 (likely integer arrays), f3 (likely character array)
- Uses memset with zero value to clear all bytes in the arrays
- Called multiple times throughout the execute_test function to ensure clean state for each test case
- The function has the same name 'reset' as many other functions throughout the PostgreSQL codebase, but this particular instance is specific to ECPG testing
- Located in src/interfaces/ecpg/test/expected/sql-declare.c:600-606
- Simple but essential for maintaining test isolation and preventing false positives/negatives in test results

## Simplified Source

```c
// Simplified version of reset
void reset() {
    // Clear all test output variables to ensure clean state
    memset(f1, 0, sizeof(f1));  // Clear first output array
    memset(f2, 0, sizeof(f2));  // Clear second output array
    memset(f3, 0, sizeof(f3));  // Clear third output array
}
```

Key simplifications made:
- Added descriptive comments for each memset operation
- Focused on the core functionality: clearing test variables
- Maintained original simple structure as no complex logic was present