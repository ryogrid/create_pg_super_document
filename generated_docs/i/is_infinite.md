# is_infinite

## Location
[src/backend/utils/adt/float.c:111-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L111-L156)

## Overview
A utility function that determines whether a double-precision floating-point value represents positive infinity, negative infinity, or a finite value.

## Definition


## Detailed Description
This function provides a portable way to detect and distinguish between positive and negative infinity in double-precision floating-point values. While C99 provides the  macro, it does not guarantee that implementations will distinguish between positive and negative infinity. This function ensures consistent behavior across all platforms by explicitly checking the sign of infinite values.

The function returns:
-  if the value represents negative infinity
-  if the value represents positive infinity  
-  if the value is finite (not infinite)

This distinction is important for mathematical operations and proper handling of infinite results in PostgreSQL's floating-point arithmetic.

## Parameters / Member Variables
- : The double-precision floating-point value to test for infinity

## Dependencies
- Functions called/Symbols referenced:
  -  (C standard library macro/function for infinity detection)
- Called from (representative examples):
  - Currently no direct callers found in the indexed codebase, but likely used internally by other floating-point functions

## Notes and Other Information
- Provides portable infinity detection across different platforms and C library implementations
- The explicit sign checking ensures consistent behavior regardless of the underlying  implementation
- Part of PostgreSQL's comprehensive floating-point utility functions
- The function handles the platform-specific variations in infinity representation and detection
- Critical for mathematical correctness when dealing with infinite results from operations like division by zero or overflow
- The three-way return value (-1, 0, 1) provides complete information about the infinity status and sign