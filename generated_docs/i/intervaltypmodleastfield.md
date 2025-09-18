# intervaltypmodleastfield

## Location
src/backend/utils/adt/timestamp.c: 1221 - 1273

## Overview
Determines the least significant (finest granularity) temporal field that an INTERVAL type modifier allows to be nonzero, returning a numeric code representing the field's significance level.

## Definition
```c
static int intervaltypmodleastfield(int32 typmod)
```

## Detailed Description
The `intervaltypmodleastfield` function analyzes an interval type modifier to identify the smallest temporal unit that can have a nonzero value according to the typmod specification. This is crucial for interval operations and comparisons, as it determines the effective resolution of the interval type.

The function implements a custom ordering system where temporal fields are ranked by significance:
- SECOND = 0 (most significant/finest granularity)  
- MINUTE = 1
- HOUR = 2
- DAY = 3
- MONTH = 4
- YEAR = 5 (least significant/coarsest granularity)

This ordering differs from the standard dt.h macros and provides a consistent way to compare interval resolution across different field combinations.

## Parameters / Member Variables
- `typmod`: 32-bit integer containing encoded interval type modifier with range and precision information

## Dependencies
- Functions called/Symbols referenced:
  - INTERVAL_RANGE
  - INTERVAL_MASK
  - INTERVAL_FULL_RANGE
  - YEAR, MONTH, DAY, HOUR, MINUTE, SECOND constants
  - elog (for error reporting)
- Called from (representative examples):
  - [interval_support](interval_support.md) (multiple calls for type system integration)

## Notes and Other Information
- Returns 0 (SECOND) for negative typmod values, representing full precision
- Uses a custom significance ordering system instead of standard dt.h macros for consistent field comparison
- Handles all valid SQL interval field combinations (single fields and ranges)
- Essential for interval type system operations that need to understand the effective resolution
- Static function, used internally within timestamp.c for interval type support
- For compound intervals (e.g., "day to second"), returns the code for the least significant field in the range