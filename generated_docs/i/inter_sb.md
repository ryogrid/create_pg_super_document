# inter_sb

## Location
[src/backend/utils/adt/geo_ops.c:3315-3327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3315-L3327)

## Overview
Tests whether a line segment intersects with a box.

## Definition

```c
struct(&bseg, &p1, &p2);
```
## Detailed Description
The  function is a PostgreSQL geometric operator that determines if a line segment (LSEG) intersects with a rectangular box (BOX). This function serves as a SQL-callable wrapper for the  function, providing a boolean result for intersection testing. The function is part of the intersection testing family (inter_*) and implements the geometric intersection operator for line segment-box relationships in PostgreSQL's spatial data type system. It delegates the actual geometric computation to the more comprehensive  function while only requesting a boolean result.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: LSEG pointer (line segment)
  - Second argument: BOX pointer (rectangular box)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract line segment from function arguments
  -  - Extract box from function arguments
  -  - Core geometric function that performs intersection testing (called with NULL for boolean-only result)
  -  - Return boolean result to PostgreSQL
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- This function implements the PostgreSQL geometric intersection operator for line segment-box relationships
- Located in the geometric operations module (geo_ops.c)
- Part of the inter_* family of intersection testing functions
- Uses NULL as first parameter to  to indicate boolean-only testing (no intersection point calculation needed)
- Considers line segments completely inside the box as intersecting
- Returns a boolean Datum indicating whether intersection occurs