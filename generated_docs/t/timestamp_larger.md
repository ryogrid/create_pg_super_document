# timestamp_larger

## Location
[src/backend/utils/adt/timestamp.c:2771-2785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2771-L2785)

## Overview
Returns the larger of two timestamp values, implementing the PostgreSQL GREATEST() function for timestamp types.

## Definition
```c
Datum timestamp_larger(PG_FUNCTION_ARGS)
```

## Detailed Description
This function compares two timestamp values and returns the chronologically later (larger) timestamp. It uses the internal timestamp comparison function to ensure consistency with other timestamp comparison operations. The function handles both finite timestamps and infinite values.

The function extracts two Timestamp arguments from the PostgreSQL function call interface, compares them using timestamp_cmp_internal(), and returns the larger value. This is commonly used in SQL queries with the GREATEST() function or in internal PostgreSQL operations requiring timestamp maximum calculations.

## Parameters / Member Variables
- `dt1`: First timestamp value to compare (from PG_GETARG_TIMESTAMP(0))
- `dt2`: Second timestamp value to compare (from PG_GETARG_TIMESTAMP(1))  
- `result`: The larger of the two input timestamps

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (PostgreSQL function call interface macro)
  - [timestamp_cmp_internal](timestamp_cmp_internal.md) (internal timestamp comparison function)
  - PG_RETURN_TIMESTAMP (PostgreSQL return value macro)
  - Timestamp (timestamp data type)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Companion function to timestamp_smaller, implementing the opposite comparison logic
- Uses timestamp_cmp_internal with > 0 comparison to find the larger timestamp
- Handles all timestamp values including infinite timestamps  
- Typically exposed as SQL function GREATEST() for timestamp types
- Located at src/backend/utils/adt/timestamp.c:2771-2785

## Simplified Source

```c
Datum timestamp_larger(PG_FUNCTION_ARGS)
{
    // Extract two timestamp arguments
    Timestamp timestamp1 = PG_GETARG_TIMESTAMP(0);
    Timestamp timestamp2 = PG_GETARG_TIMESTAMP(1);

    // Compare timestamps and return the larger one
    if (timestamp_cmp_internal(timestamp1, timestamp2) > 0)
        PG_RETURN_TIMESTAMP(timestamp1);
    else
        PG_RETURN_TIMESTAMP(timestamp2);
}
```