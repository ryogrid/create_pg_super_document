# interval_pl

## Location
src/backend/utils/adt/timestamp.c: 3462 - 3502

## Overview
The `interval_pl` function implements the addition operator (+) for PostgreSQL interval data types, handling both finite and infinite interval values.

## Definition
```c
Datum interval_pl(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_pl` function serves as the PostgreSQL SQL function interface for adding two interval values together. It provides comprehensive handling of both finite and infinite intervals, implementing the mathematical rules for interval arithmetic while preventing undefined operations like "infinity - infinity".

The function handles several cases:
- **NOBEGIN + NOEND**: Error (undefined operation)
- **NOEND + NOBEGIN**: Error (undefined operation)  
- **NOBEGIN + finite**: Result is NOBEGIN
- **NOEND + finite**: Result is NOEND
- **finite + NOBEGIN/NOEND**: Result inherits the infinity
- **finite + finite**: Delegates to `finite_interval_pl` for safe arithmetic

The function follows PostgreSQL's standard function interface pattern, extracting arguments, allocating memory for results, and returning through the standard mechanism.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `span1`: First interval value to add
  - `span2`: Second interval value to add

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INTERVAL_P` - Extract interval arguments from function call
  - [palloc](../p/palloc.md) - Allocate memory for result interval
  - `INTERVAL_IS_NOBEGIN` - Check if interval represents negative infinity
  - `INTERVAL_IS_NOEND` - Check if interval represents positive infinity
  - `INTERVAL_NOT_FINITE` - Check if interval is infinite (either direction)
  - `INTERVAL_NOBEGIN` - Set result to negative infinity
  - `INTERVAL_NOEND` - Set result to positive infinity
  - `memcpy` - Copy interval data for infinite values
  - [finite_interval_pl](../f/finite_interval_pl.md) - Perform addition of finite intervals
  - `ereport` - PostgreSQL error reporting function
  - `PG_RETURN_INTERVAL_P` - Return the result interval
- Called from (representative examples):
  - [interval_lerp](interval_lerp.md) - Interval linear interpolation (src/backend/utils/adt/orderedsetaggs.c:519)
  - [in_range_interval_interval](in_range_interval_interval.md) - Range checking function (src/backend/utils/adt/timestamp.c:3906)

## Notes and Other Information
- Implements PostgreSQL's + operator for intervals in SQL expressions
- Prevents undefined operations (NOBEGIN + NOEND, NOEND + NOBEGIN) by raising errors
- Uses mathematical infinity rules: finite + infinity = infinity
- Delegates finite arithmetic to `finite_interval_pl` for overflow protection
- Allocates new memory for results rather than modifying inputs
- Raises ERROR with ERRCODE_DATETIME_VALUE_OUT_OF_RANGE for undefined operations
- Core component of PostgreSQL's interval arithmetic system