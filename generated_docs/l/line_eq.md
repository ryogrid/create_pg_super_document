# line_eq

## Location
src/backend/utils/adt/geo_ops.c: 1194 - 1232

## Overview
Compares two LINE objects for equality by checking if their parameters are proportional, representing the same geometric line.

## Definition
Datum line_eq(PG_FUNCTION_ARGS)

## Detailed Description
This function determines whether two lines are geometrically equivalent by checking if their coefficients in the standard line equation Ax + By + C = 0 are proportional. Two lines are considered equal if one can be obtained from the other by multiplying all coefficients by the same non-zero constant. The function handles special cases like NaN values by requiring exact equality, and uses floating-point comparison with appropriate tolerance for numerical precision.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function argument macro providing access to:
  - l1 (LINE*): Pointer to the first LINE structure to compare
  - l2 (LINE*): Pointer to the second LINE structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - LINE (data type)
  - PG_GETARG_LINE_P (argument extraction macro)
  - isnan (NaN detection function)
  - float8_eq (exact floating-point equality)
  - FPzero (floating-point zero comparison)
  - float8_div (floating-point division)
  - float8_mul (floating-point multiplication)
  - FPeq (floating-point equality with tolerance)
  - PG_RETURN_BOOL (return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Uses proportionality check rather than exact coefficient equality to handle equivalent line representations
- Special handling for NaN values requires exact equality of all coefficients
- Calculates proportionality ratio using the first non-zero coefficient from the second line
- Uses floating-point comparison with tolerance for robustness against numerical precision issues
- Part of PostgreSQLs geometric data type operations for LINE comparisons
- Returns boolean result indicating whether the two lines represent the same geometric line