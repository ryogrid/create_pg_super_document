# on_sb

## Location
[src/backend/utils/adt/geo_ops.c:3224-3237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3224-L3237)

## Overview
Tests whether a line segment lies on or within a box boundary.

## Definition


## Detailed Description
The  function is a PostgreSQL geometric operator that determines if a line segment (LSEG) is positioned on or within the boundaries of a box (BOX). This function serves as a SQL-callable wrapper that implements the "on" spatial relationship between line segments and boxes. The function leverages the existing  function to perform the actual geometric calculation, returning true if the line segment is contained within or lies on the box boundaries.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: LSEG pointer (line segment)
  - Second argument: BOX pointer (rectangular box)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract line segment from function arguments
  -  - Extract box from function arguments  
  -  - Core geometric function that tests containment
  -  - Return boolean result to PostgreSQL
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Notes and Other Information
- This function implements the PostgreSQL geometric "@" operator for line segment-box relationships
- Located in the geometric operations module (geo_ops.c)
- Returns a boolean Datum indicating spatial relationship
- The actual geometric computation is delegated to  for code reuse