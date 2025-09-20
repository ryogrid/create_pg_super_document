# on_sl

## Location
[src/backend/utils/adt/geo_ops.c:3201-3216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3201-L3216)

## Overview
This function tests whether a line segment lies on a line, by checking if both endpoints of the segment lie on the line.

## Definition

```c
Datum
on_sl(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function determines whether a line segment is positioned on a line (or close enough to be considered on the line). The algorithm is straightforward: it checks if both endpoints of the line segment lie on the specified line using the  function. If both endpoints are on the line, then the entire segment must also be on the line, assuming the line is infinite and straight.

The function accounts for floating-point precision by using a containment test that includes a tolerance for "close enough" positioning.

## Parameters / Member Variables
- : PostgreSQL function call context containing:
  - Argument 0:  - The line segment to test
  - Argument 1:  - The line to test against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts line segment argument from function call
  -  - Extracts line argument from function call
  -  - Tests if a point lies on the line (called twice, once for each endpoint)
  -  - Returns boolean result to PostgreSQL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL's geometric data type operators for line-segment relationships
- The function name 'on_sl' likely stands for 'on segment-line'
- Uses a simple but effective algorithm: if both endpoints are on the line, the entire segment is on the line
- Located in src/backend/utils/adt/geo_ops.c:3201-3216
- Handles floating-point precision issues through the  function's tolerance mechanisms
- Returns true only if both endpoints of the segment lie on the line