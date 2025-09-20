# timestamp_cmp

## Location
[src/backend/utils/adt/timestamp.c:2270-2280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2270-L2280)

## Overview
Compares two timestamp values and returns an integer indicating their relative order (-1, 0, or 1).

## Definition
Datum timestamp_cmp(PG_FUNCTION_ARGS)

## Detailed Description
This function implements the comparison function for PostgreSQL timestamp values, returning an integer that indicates the relative ordering of two timestamps. It extracts two timestamp arguments from the function call arguments using PostgreSQL function argument macros, then delegates the actual comparison logic to timestamp_cmp_internal and returns the comparison result as a 32-bit integer. This function serves as the basis for all timestamp comparison operations and is used by PostgreSQL's comparison operators and sorting algorithms.

## Parameters / Member Variables
- Argument 0: First timestamp value to compare
- Argument 1: Second timestamp value to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP (macro to extract timestamp arguments)
  - [timestamp_cmp_internal](timestamp_cmp_internal.md) (internal comparison function)
  - PG_RETURN_INT32 (macro to return 32-bit integer result)
  - SIZEOF_DATUM (referenced in context)
- Called from (representative examples):
  - [compareDatetime](../c/compareDatetime.md) (in src/backend/utils/adt/jsonpath_exec.c:3822)
  - [compareDatetime](../c/compareDatetime.md) (in src/backend/utils/adt/jsonpath_exec.c:3856)

## Notes and Other Information
- This function is the core comparison function for timestamp data type
- Returns -1 if first timestamp is less than second, 0 if equal, 1 if greater
- Used by PostgreSQL's sorting and indexing mechanisms for timestamp columns
- Part of PostgreSQL's SQL operator implementation framework
- Located in src/backend/utils/adt/timestamp.c:2270-2280