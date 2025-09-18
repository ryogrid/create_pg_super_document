# chareq

## Location
[src/backend/utils/adt/char.c:127-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/char.c#L127-L135)

## Overview
Compares two character values for equality, treating them as unsigned 8-bit integers for comparison purposes.

## Definition


## Detailed Description
The chareq function is the equality comparison operator for PostgreSQL's "char" data type. It implements the "=" operator for single-character comparisons within SQL expressions and queries.

The function performs a simple byte-wise equality comparison between two character values. According to the code comments, comparisons are performed as though the char values are unsigned (uint8), which ensures consistent behavior across all character values from 0x00 to 0xFF.

This function is part of PostgreSQL's operator infrastructure and is automatically invoked when the "=" operator is used with "char" type operands in SQL queries, WHERE clauses, joins, and other comparison contexts.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - First character value for comparison
  - Second character value for comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CHAR (to extract the first character argument)
  - PG_GETARG_CHAR (to extract the second character argument)  
  - PG_RETURN_BOOL (to return the boolean comparison result)
- Called from (representative examples):
  - SQL queries with "char" equality comparisons (e.g., WHERE col = 'x')
  - PostgreSQL operator evaluation during query execution
  - Index operations requiring character equality tests

## Notes and Other Information
- Comparison is performed treating characters as unsigned 8-bit values (0-255 range)
- This differs from integer conversion operations which treat char as signed (int8)
- The inconsistency between comparison (unsigned) and conversion (signed) semantics is acknowledged in the source comments
- Returns a PostgreSQL boolean datum (true/false)
- Part of a complete set of comparison operators for the "char" data type
- Used internally by PostgreSQL's query planner and executor for optimization and evaluation