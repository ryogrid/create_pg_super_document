# time_eq

## Location
[src/backend/utils/adt/date.c:1680-1688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1680-L1688)

## Overview
A PostgreSQL function that performs equality comparison between two TIME values, returning true if they are identical.

## Definition
```c
Datum time_eq(PG_FUNCTION_ARGS)
```

## Detailed Description
The time_eq function implements the equality operator (=) for TIME data types in PostgreSQL. It performs a direct bitwise comparison between two TimeADT values to determine if they represent the same time. Since TimeADT is internally represented as microseconds since midnight, this comparison is straightforward and efficient. The function is part of PostgreSQL's operator infrastructure and is typically called through SQL equality expressions rather than directly.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro
  - `time1`: TimeADT value representing the first time to compare
  - `time2`: TimeADT value representing the second time to compare

## Dependencies
- Functions called/Symbols referenced:
  - TimeADT (data type for time values)
  - PG_GETARG_TIMEADT (macro for extracting TimeADT arguments)
  - PG_RETURN_BOOL (macro for returning boolean results)
- Called from (representative examples):
  - No direct references found in the codebase (used through SQL operator system)

## Notes and Other Information
- Performs simple bitwise equality comparison since TimeADT values are stored as int64 microseconds
- Part of PostgreSQL's complete set of comparison operators for TIME data types
- Used internally by the SQL engine when processing equality conditions in WHERE clauses, JOIN conditions, and other comparison contexts
- Located in src/backend/utils/adt/date.c:1680-1688
- Returns a PostgreSQL boolean Datum that can be used in SQL expressions
- Complemented by time_ne for inequality comparisons