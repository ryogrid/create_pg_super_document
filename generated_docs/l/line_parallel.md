# line_parallel

## Location
src/backend/utils/adt/geo_ops.c: 1146 - 1154

## Overview
Determines whether two LINE objects are parallel in PostgreSQL's geometric data type system.

## Definition


## Detailed Description
This function checks if two LINE objects are parallel by negating the result of the intersection test. Two lines are considered parallel if they do not intersect (excluding the case where they are the same line). The function uses the internal  helper function and returns the logical NOT of its result - if the lines don't intersect, they are parallel.

## Parameters / Member Variables
- : PostgreSQL function argument macro that expands to access two LINE parameters:
  - First line (l1): First line for parallel test
  - Second line (l2): Second line for parallel test

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LINE_P: Extracts LINE arguments from function call
  - line_interpt_line: Internal helper to test line intersection
  - PG_RETURN_BOOL: Returns boolean result
- Called from (representative examples):
  - No direct callers found (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1146-1154
- Part of PostgreSQL's relative position routines for geometric operations
- Returns true if lines are parallel (do not intersect), false if they intersect
- The function follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS macro
- Uses NULL as first parameter to line_interpt_line to indicate intersection test only
- Implements parallel test as the logical inverse of intersection test