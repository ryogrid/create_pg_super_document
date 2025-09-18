# circle_same

## Location
src/backend/utils/adt/geo_ops.c: 4751 - 4763

## Overview
Tests whether two circles are identical by comparing both their centers and radii, treating NaN values as equal to enable proper handling of circles with undefined radii.

## Definition


## Detailed Description
The  function determines if two CIRCLE objects are identical by performing a comprehensive comparison of their geometric properties. The function implements a special equality semantics where NaN (Not a Number) radius values are considered equal to each other, which allows circles with undefined radii to be properly identified as matching. This is crucial for PostgreSQL's geometric operations where NaN values need to be handled consistently.

The comparison process involves two main checks:
1. Radius comparison using either NaN-aware equality (both NaN) or floating-point equality (FPeq)
2. Center point comparison using the existing point_eq_point function

This function serves as the implementation for the PostgreSQL SQL operator  for circle types.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : First CIRCLE object to compare (accessed via PG_GETARG_CIRCLE_P(0))
  - : Second CIRCLE object to compare (accessed via PG_GETARG_CIRCLE_P(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P: Extracts CIRCLE arguments from function call
  - isnan: Checks if radius values are NaN
  - FPeq: Floating-point equality comparison for radii
  - point_eq_point: Compares circle center points for equality
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL queries using the  operator for circles
  - Internal geometric comparison operations

## Notes and Other Information
- Part of PostgreSQL's geometric data type system for 2D circle operations
- Implements special NaN semantics where NaN == NaN evaluates to true, unlike standard floating-point behavior
- Used as the underlying implementation for the circle equality operator  in SQL
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS and PG_RETURN_BOOL macros
- Located in the geometric operations module alongside other circle comparison functions