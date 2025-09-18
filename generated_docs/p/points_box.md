# points_box

## Location
src/backend/utils/adt/geo_ops.c: 4217 - 4230

## Overview
The points_box function creates a bounding box (BOX) from two Point objects, constructing the smallest rectangle that contains both points.

## Definition
Datum points_box(PG_FUNCTION_ARGS)

## Detailed Description
This function constructs a PostgreSQL BOX geometric object from two Point arguments. The function serves as a PostgreSQL-callable interface that takes two points and creates a rectangular bounding box that encompasses both points. It allocates memory for a new BOX object and uses the box_construct helper function to determine the proper high and low coordinates based on the input points. The resulting box represents the smallest axis-aligned rectangle that contains both input points.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function arguments containing:
  - Argument 0: First Point (p1) - one corner of the box
  - Argument 1: Second Point (p2) - opposite corner of the box

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P (argument extraction)
  - palloc (memory allocation)
  - box_construct (constructs the BOX from the two points)
  - PG_RETURN_BOX_P (return value packaging)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Marks the beginning of the 2D box routines section in geo_ops.c
- The function allocates memory for a new BOX result using palloc
- Uses box_construct to handle coordinate ordering (determining high/low values)
- Returns a PostgreSQL Datum containing the result BOX
- Part of PostgreSQL's geometric data type conversion operations
- Located in src/backend/utils/adt/geo_ops.c at lines 4217-4230