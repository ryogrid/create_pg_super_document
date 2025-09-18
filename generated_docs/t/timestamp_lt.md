# timestamp_lt

## Location
src/backend/utils/adt/timestamp.c: 2234 - 2242

## Overview
PostgreSQL function that implements the less-than operator (<) for timestamp values, returning true if the first timestamp is earlier than the second.

## Definition
```c
Datum timestamp_lt(PG_FUNCTION_ARGS)
```

## Detailed Description
timestamp_lt is a PostgreSQL built-in function that implements the less-than comparison operator for timestamp data types. It follows PostgreSQL's function calling convention using PG_FUNCTION_ARGS to receive arguments and returns a Datum. The function extracts two Timestamp arguments, compares them using the internal timestamp_cmp_internal function, and returns true if the first timestamp is earlier than the second (comparison result is less than 0). This function is typically invoked through SQL's < operator when comparing timestamp values for chronological ordering.

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
  - Direct SQL usage through the < operator for timestamp comparisons
  - Internal PostgreSQL query execution engine
  - No direct code references found in the analyzed codebase

## Notes and Other Information
- Part of PostgreSQL's SQL operator system, typically invoked through SQL < operator
- Returns PostgreSQL boolean type (true/false) wrapped as Datum
- Leverages the shared timestamp_cmp_internal function for actual comparison logic
- Function signature follows PostgreSQL's version-1 calling convention
- Used for WHERE clauses, ORDER BY clauses, JOIN conditions, and other SQL comparisons involving timestamp ordering
- Essential for temporal queries and chronological sorting operations
- Performance is dependent on the underlying timestamp_cmp_internal implementation
- Works with PostgreSQL's indexing system for efficient range queries on timestamp columns