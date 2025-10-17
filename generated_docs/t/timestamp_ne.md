# timestamp_ne

## Location
[src/backend/utils/adt/timestamp.c:2225-2233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2225-L2233)

## Overview
PostgreSQL function that implements the inequality operator (<> or !=) for timestamp values, returning true if two timestamps are not equal.

## Definition
```c
Datum timestamp_ne(PG_FUNCTION_ARGS)
```

## Detailed Description
timestamp_ne is a PostgreSQL built-in function that implements the inequality comparison operator for timestamp data types. It follows PostgreSQL's function calling convention using PG_FUNCTION_ARGS to receive arguments and returns a Datum. The function extracts two Timestamp arguments, compares them using the internal timestamp_cmp_internal function, and returns true if they are not equal (comparison result does not equal 0). This function is typically invoked through SQL's <> or != operators when comparing timestamp values for inequality.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention:
  - Argument 0: First Timestamp value (extracted via PG_GETARG_TIMESTAMP(0))
  - Argument 1: Second Timestamp value (extracted via PG_GETARG_TIMESTAMP(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP: Macro to extract Timestamp arguments from function call
  - [timestamp_cmp_internal](timestamp_cmp_internal.md): Internal comparison function
  - PG_RETURN_BOOL: Macro to return boolean result as Datum
  - Timestamp: PostgreSQL's internal timestamp data type

- Called from (representative examples):
  - Direct SQL usage through the <> or != operators for timestamp comparisons
  - Internal PostgreSQL query execution engine
  - No direct code references found in the analyzed codebase

## Notes and Other Information
- Part of PostgreSQL's SQL operator system, typically invoked through SQL <> or != operators
- Returns PostgreSQL boolean type (true/false) wrapped as Datum
- Leverages the shared timestamp_cmp_internal function for actual comparison logic
- Function signature follows PostgreSQL's version-1 calling convention
- Used for WHERE clauses, JOIN conditions, and other SQL inequality comparisons involving timestamps
- Logical complement of timestamp_eq function
- Performance is dependent on the underlying timestamp_cmp_internal implementation

## Simplified Source

```c
Datum
timestamp_ne(PG_FUNCTION_ARGS)
{
    Timestamp dt1 = PG_GETARG_TIMESTAMP(0);
    Timestamp dt2 = PG_GETARG_TIMESTAMP(1);

    // Return true if timestamps are not equal
    PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) != 0);
}
```