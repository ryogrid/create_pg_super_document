# circle_sub_pt

## Location
[src/backend/utils/adt/geo_ops.c:4980-4998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4980-L4998)

## Overview
Translates a circle by subtracting the coordinates of a point from the circle's center, returning a new circle with the same radius but at the translated position.

## Definition
```c
Datum circle_sub_pt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_sub_pt` function implements the inverse translation operator for PostgreSQL's CIRCLE data type. It performs a geometric translation by subtracting a point's coordinates from the circle's center coordinates, effectively moving the circle to a new position while preserving its radius. The function allocates memory for a new CIRCLE result, uses the `point_sub_point` helper function to compute the new center coordinates by subtracting the input point from the original center, copies the original radius unchanged, and returns the translated circle.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: CIRCLE pointer (circle) - the original circle to translate
  - Argument 1: Point pointer (point) - the translation vector to subtract

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P (retrieves CIRCLE argument)
  - PG_GETARG_POINT_P (retrieves Point argument)
  - [palloc](../p/palloc.md) (allocates memory for result)
  - [point_sub_point](../p/point_sub_point.md) (subtracts point coordinates to compute new center)
  - PG_RETURN_CIRCLE_P (returns CIRCLE result)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric "arithmetic" operators for circles
- Performs geometric translation in the opposite direction of circle_add_pt
- The radius remains unchanged during translation
- Memory is allocated for the result using PostgreSQL's memory management (palloc)
- Located in src/backend/utils/adt/geo_ops.c:4980-4998
- Translation is implemented as vector subtraction: new_center = old_center - translation_vector
- Complementary function to circle_add_pt, providing bidirectional translation capability