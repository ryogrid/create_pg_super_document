# line_intersect

## Location
src/backend/utils/adt/geo_ops.c: 1137 - 1145

## Overview
Determines whether two LINE objects intersect in PostgreSQL's geometric data type system.

## Definition


## Detailed Description
This function checks if two LINE objects intersect by using the internal  helper function. It takes two LINE arguments and returns a boolean value indicating whether the lines intersect. The function delegates the actual intersection calculation to , passing NULL as the first parameter to indicate that only the intersection test (not the intersection point) is needed.

## Parameters / Member Variables
- : PostgreSQL function argument macro that expands to access two LINE parameters:
  - First line (l1): First line for intersection test
  - Second line (l2): Second line for intersection test

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LINE_P: Extracts LINE arguments from function call
  - [line_interpt_line](line_interpt_line.md): Internal helper to test/calculate line intersection
  - PG_RETURN_BOOL: Returns boolean result
- Called from (representative examples):
  - No direct callers found (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1137-1145
- Part of PostgreSQL's relative position routines for geometric operations
- Returns true if lines intersect, false if they are parallel and don't intersect
- The function follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS macro
- Uses NULL as first parameter to line_interpt_line to indicate intersection test only