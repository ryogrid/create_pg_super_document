# circle_add_pt

## Location
src/backend/utils/adt/geo_ops.c: 4965 - 4979

## Overview
Translates a circle by adding the coordinates of a point to the circle's center, returning a new circle with the same radius but at the translated position.

## Definition
```c
Datum circle_add_pt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_add_pt` function implements the translation operator for PostgreSQL's CIRCLE data type. It performs a geometric translation by adding a point's coordinates to the circle's center coordinates, effectively moving the circle to a new position while preserving its radius. The function allocates memory for a new CIRCLE result, uses the `point_add_point` helper function to compute the new center coordinates by adding the input point to the original center, copies the original radius unchanged, and returns the translated circle.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: CIRCLE pointer (circle) - the original circle to translate
  - Argument 1: Point pointer (point) - the translation vector

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P (retrieves CIRCLE argument)
  - PG_GETARG_POINT_P (retrieves Point argument)
  - palloc (allocates memory for result)
  - point_add_point (adds point coordinates to compute new center)
  - PG_RETURN_CIRCLE_P (returns CIRCLE result)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric "arithmetic" operators for circles
- Performs geometric translation, not mathematical addition
- The radius remains unchanged during translation
- Memory is allocated for the result using PostgreSQL's memory management (palloc)
- Located in src/backend/utils/adt/geo_ops.c:4965-4979
- Translation is implemented as vector addition: new_center = old_center + translation_vector
- Part of the geometric operator family alongside circle_sub_pt