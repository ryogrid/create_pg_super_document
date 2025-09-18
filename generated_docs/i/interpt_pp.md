# interpt_pp

## Location
src/test/regress/regress.c: 90 - 130

## Overview
The interpt_pp function finds the intersection point between two paths (polygonal lines) in PostgreSQL's regression testing framework.

## Definition


## Detailed Description
This function takes two PATH objects as input and determines if any line segments from the first path intersect with any line segments from the second path. It uses a nested loop approach to check all possible combinations of line segments between the two paths. When an intersection is found, it returns the exact intersection point using the lseg_interpt function. If no intersection exists, the function returns NULL.

The function is part of PostgreSQL's regression testing suite, specifically designed to test geometric operations involving path intersections.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  - First argument: PATH pointer (p1) - the first path to check for intersections
  - Second argument: PATH pointer (p2) - the second path to check for intersections

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P (macro for extracting PATH arguments)
  - regress_lseg_construct (constructs line segments from points)
  - DirectFunctionCall2 (calls PostgreSQL functions directly)
  - lseg_intersect (checks if two line segments intersect)
  - lseg_interpt (finds intersection point of two line segments)
  - LsegPGetDatum (converts LSEG pointer to Datum)
  - PG_RETURN_DATUM (returns Datum value)
  - PG_RETURN_NULL (returns NULL value)
- Data types used:
  - PATH (geometric path type)
  - LSEG (line segment type)
  - Datum (PostgreSQL's generic data type)

- Called from (representative examples):
  - DELIM (referenced in the same file)

## Notes and Other Information
- This function is specifically used in PostgreSQL's regression testing framework
- Uses a brute-force approach with nested loops to find intersections
- Returns the intersection point of the first pair of intersecting segments found
- The function assumes that if lseg_intersect returns true, lseg_interpt will not return NULL
- Located in src/test/regress/regress.c, indicating it's part of the test suite rather than core functionality