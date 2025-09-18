# inter_sl

## Location
[src/backend/utils/adt/geo_ops.c:3238-3262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3238-L3262)

## Overview
Tests whether a line segment intersects with an infinite line.

## Definition


## Detailed Description
The  function is a PostgreSQL geometric operator that determines if a line segment (LSEG) intersects with an infinite line (LINE). This function is part of the intersection testing family (inter_*) and serves as a SQL-callable wrapper for geometric intersection calculations. The function uses  with a NULL first parameter to perform a boolean intersection test without calculating the actual intersection point, returning true if the line segment and infinite line intersect anywhere.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: LSEG pointer (line segment)
  - Second argument: LINE pointer (infinite line)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract line segment from function arguments
  -  - Extract line from function arguments
  -  - Core geometric function that tests intersection (called with NULL for boolean-only result)
  -  - Return boolean result to PostgreSQL
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- This function implements the PostgreSQL geometric intersection operator for line segment-line relationships
- Located in the geometric operations module (geo_ops.c)
- Part of the inter_* family of intersection testing functions
- Uses NULL as first parameter to  to indicate boolean-only testing (no intersection point calculation needed)
- Returns a boolean Datum indicating whether intersection occurs