# time_ne

## Location
src/backend/utils/adt/date.c: 1689 - 1697

## Overview
A PostgreSQL function that performs inequality comparison between two TIME values, returning true if they are different.

## Definition
```c
Datum time_ne(PG_FUNCTION_ARGS)
```

## Detailed Description
The time_ne function implements the inequality operator (<> or !=) for TIME data types in PostgreSQL. It performs a direct bitwise comparison between two TimeADT values to determine if they represent different times. Since TimeADT is internally represented as microseconds since midnight, this comparison is straightforward and efficient. The function is part of PostgreSQL's operator infrastructure and is typically called through SQL inequality expressions rather than directly. It provides the logical complement to the time_eq function.

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
- Performs simple bitwise inequality comparison since TimeADT values are stored as int64 microseconds
- Part of PostgreSQL's complete set of comparison operators for TIME data types
- Used internally by the SQL engine when processing inequality conditions in WHERE clauses, JOIN conditions, and other comparison contexts
- Located in src/backend/utils/adt/date.c:1689-1697
- Returns a PostgreSQL boolean Datum that can be used in SQL expressions
- Provides the logical complement to time_eq for complete equality/inequality comparison support
- Essential for SQL operations like NOT EQUAL, exclusion constraints, and negated equality conditions