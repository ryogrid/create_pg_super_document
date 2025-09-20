# on_pb

## Location
[src/backend/utils/adt/geo_ops.c:3137-3145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3137-L3145)

## Overview
This function tests whether a point is contained within (or lies on the boundary of) a box, serving as a PostgreSQL geometric operator.

## Definition

```c
Datum
on_pb(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL geometric operator that determines if a point is contained within a box or lies on its boundary. It's implemented as a PostgreSQL function that takes two arguments (a point and a box) and returns a boolean result. The function acts as a wrapper around the  function, providing the geometric relationship test between a point and a box.

## Parameters / Member Variables
- : PostgreSQL function call context containing:
  - Argument 0:  - The point to test for containment
  - Argument 1:  - The box to test against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts point argument from function call
  -  - Extracts box argument from function call
  -  - Core function that performs the containment test
  -  - Returns boolean result to PostgreSQL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operators
- The function name 'on_pb' likely stands for 'on point-box'
- Located in src/backend/utils/adt/geo_ops.c:3137-3145
- Returns true if the point is inside the box or on its boundary