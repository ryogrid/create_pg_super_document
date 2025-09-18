# numeric_is_nan

## Location
[src/backend/utils/adt/numeric.c:849-859](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L849-L859)

## Overview
A utility function that tests whether a Numeric value represents "Not a Number" (NaN).

## Definition
```c
bool numeric_is_nan(Numeric num)
```

## Detailed Description
The `numeric_is_nan` function provides a simple boolean test to determine if a Numeric value is NaN (Not a Number). This is a lightweight wrapper around the `NUMERIC_IS_NAN` macro that offers a function interface for checking NaN status. 

NaN values in PostgreSQL's numeric type represent undefined or invalid mathematical results (such as 0/0 or infinity - infinity). This function is essential for:

1. **Input validation**: Ensuring numeric values are valid before processing
2. **Mathematical operations**: Handling edge cases in arithmetic functions  
3. **JSON operations**: Proper handling of NaN values in JSON contexts
4. **LSN arithmetic**: Validating log sequence number calculations

## Parameters / Member Variables
- `num`: The Numeric value to test for NaN status

## Dependencies
- Functions called/Symbols referenced:
  - `NUMERIC_IS_NAN`: Macro that performs the actual NaN check on the Numeric structure
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md): JSON path execution with NaN handling
  - [pg_lsn_pli](../p/pg_lsn_pli.md): PostgreSQL LSN (Log Sequence Number) arithmetic operations
  - [pg_lsn_mii](../p/pg_lsn_mii.md): LSN arithmetic with NaN validation
  - Various numeric utility functions via `PG_RETURN_NUMERIC` header

## Notes and Other Information
- Simple wrapper function providing a clean API for NaN detection
- Returns `true` if the value is NaN, `false` otherwise
- Used extensively in JSON operations and LSN arithmetic where NaN values need special handling
- Part of PostgreSQL's comprehensive support for IEEE 754-style special numeric values
- The function is inline-friendly and very lightweight, essentially just a macro call