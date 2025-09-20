# point_eq

## Location
[src/backend/utils/adt/geo_ops.c:1955-1963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1955-L1963)

## Overview
Tests whether two points are equal by comparing both their x and y coordinates using floating-point tolerance.

## Definition

```c
Datum
point_eq(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL geometric operator that determines if two points are equal. It serves as a wrapper function that extracts two Point arguments from the PostgreSQL function call interface and delegates the actual comparison logic to the  function. This function is part of PostgreSQL's geometric data type system and supports equality operations in SQL queries.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: Point pointer () - the first point to compare
  - Second argument: Point pointer () - the second point to compare

## Dependencies
- Functions called/Symbols referenced:
  -  - extracts Point arguments from function call
  -  - performs the actual point equality comparison
  -  - returns boolean result to PostgreSQL
- Called from (representative examples):
  -  (in SP-GiST quadtree leaf consistency checking)

## Notes and Other Information
- This function acts as a PostgreSQL SQL-callable wrapper around the internal  function
- Used in SQL equality operations between point values (e.g., )
- Part of the SP-GiST spatial indexing system for geometric queries
- Returns true if both x and y coordinates are equal within floating-point tolerance