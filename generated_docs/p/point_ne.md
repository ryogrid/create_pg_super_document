# point_ne

## Location
[src/backend/utils/adt/geo_ops.c:1964-1976](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1964-L1976)

## Overview
Tests whether two points are not equal by comparing both their x and y coordinates using floating-point tolerance.

## Definition

```c
Datum
point_ne(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL geometric operator that determines if two points are not equal. It serves as a wrapper function that extracts two Point arguments from the PostgreSQL function call interface and negates the result of the  function. This function is part of PostgreSQL's geometric data type system and supports inequality operations in SQL queries.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: Point pointer () - the first point to compare
  - Second argument: Point pointer () - the second point to compare

## Dependencies
- Functions called/Symbols referenced:
  -  - extracts Point arguments from function call
  -  - performs the actual point equality comparison (result is negated)
  -  - returns boolean result to PostgreSQL
- Called from (representative examples):
  - Currently no direct references found in the codebase

## Notes and Other Information
- This function acts as a PostgreSQL SQL-callable wrapper for point inequality operations
- Used in SQL inequality operations between point values (e.g., )
- Implements the logical negation of the equality test performed by 
- Returns true if either x or y coordinates differ beyond floating-point tolerance
- Part of the complete set of comparison operators for the Point geometric type