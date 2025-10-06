# numeric_gt

## Location
[src/backend/utils/adt/numeric.c:2461-2475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L2461-L2475)

## Overview
PostgreSQL function that compares two numeric values and returns true if the first value is greater than the second.

## Definition

```c
Datum
numeric_gt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the greater-than comparison operator (>) for PostgreSQL's NUMERIC data type. This function is part of the comprehensive set of numeric comparison operators in PostgreSQL and serves as the backend implementation for SQL expressions like . 

The function extracts two NUMERIC arguments from the function call arguments, delegates the actual comparison logic to the  helper function, and returns a boolean result indicating whether the first numeric value is greater than the second. It properly handles memory management by freeing any copied numeric values before returning.

## Parameters / Member Variables
- Function arguments accessed via  macro:
  - First argument (index 0): First NUMERIC value for comparison
  - Second argument (index 1): Second NUMERIC value for comparison

## Dependencies
- Functions called/Symbols referenced:
  -  (macro to extract NUMERIC arguments)
  -  (core comparison logic function)
  -  (memory management macro)
  -  (macro to return boolean result)
- Called from:
  - SQL greater-than operator expressions
  - PostgreSQL operator dispatch system
  - [Numeric](../N/Numeric.md) comparison operations

## Notes and Other Information
- The function follows PostgreSQL's standard function calling convention using 
- Memory management is handled through  to ensure proper cleanup of potentially large numeric values
- The actual comparison logic is centralized in , which handles special cases like NaN and infinity values
- Part of the complete set of numeric comparison operators (=, <>, <, <=, >, >=)
- Located in src/backend/utils/adt/numeric.c

## Simplified Source

```c
Datum numeric_gt(PG_FUNCTION_ARGS) {
    // Get the two numeric arguments
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);

    // Compare the numbers and check if first > second
    bool result = cmp_numerics(num1, num2) > 0;

    // Clean up memory and return result
    PG_FREE_IF_COPY(num1, 0);
    PG_FREE_IF_COPY(num2, 1);
    PG_RETURN_BOOL(result);
}
``` 