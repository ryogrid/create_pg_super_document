# line_distance

## Location
[src/backend/utils/adt/geo_ops.c:1261-1285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1261-L1285)

## Overview
Calculates the shortest distance between two lines, returning 0.0 if they intersect or the perpendicular distance if they are parallel.

## Definition
Datum line_distance(PG_FUNCTION_ARGS)

## Detailed Description
This function computes the distance between two lines using geometric principles. If the lines intersect, the distance is 0.0. For parallel lines, it calculates the perpendicular distance using the formula based on the difference in their constant terms normalized by the magnitude of the direction vector. The function first checks for intersection using line_interpt_line(), then for parallel lines computes the distance as |C1 - ratio*C2| / sqrt(A^2 + B^2), where ratio accounts for different coefficient scaling between the parallel lines.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: PostgreSQL function argument macro providing access to:
  - l1 (LINE*): Pointer to the first LINE structure
  - l2 (LINE*): Pointer to the second LINE structure

## Dependencies
- Functions called/Symbols referenced:
  - LINE (data type)
  - PG_GETARG_LINE_P (argument extraction macro)
  - [line_interpt_line](line_interpt_line.md) (line intersection test function)
  - FPzero (floating-point zero comparison macro)
  - isnan (NaN detection function)
  - [float8_div](../f/float8_div.md) (floating-point division)
  - [float8_mi](../f/float8_mi.md) (floating-point subtraction)
  - [float8_mul](../f/float8_mul.md) (floating-point multiplication)
  - fabs (absolute value function)
  - HYPOT (hypotenuse calculation macro)
  - PG_RETURN_FLOAT8 (return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns 0.0 for intersecting lines as they have zero distance between them
- For parallel lines, uses the perpendicular distance formula from computational geometry
- Handles different coefficient scaling by calculating an appropriate ratio between line parameters
- Uses robust floating-point arithmetic with NaN checking for numerical stability
- Part of PostgreSQLs geometric data type operations for LINE distance calculations
- The distance calculation uses the point-to-line distance formula adapted for line-to-line distance

## Simplified Source

```c
Datum line_distance(PG_FUNCTION_ARGS) {
    LINE *l1 = PG_GETARG_LINE_P(0);
    LINE *l2 = PG_GETARG_LINE_P(1);
    float8 ratio;

    // If lines intersect, distance is 0
    if (line_interpt_line(NULL, l1, l2))
        PG_RETURN_FLOAT8(0.0);

    // Calculate coefficient ratio for parallel lines
    if (!FPzero(l1->A) && !isnan(l1->A) && !FPzero(l2->A) && !isnan(l2->A))
        ratio = float8_div(l1->A, l2->A);
    else if (!FPzero(l1->B) && !isnan(l1->B) && !FPzero(l2->B) && !isnan(l2->B))
        ratio = float8_div(l1->B, l2->B);
    else
        ratio = 1.0;

    // Return perpendicular distance: |C1 - ratio*C2| / sqrt(A^2 + B^2)
    PG_RETURN_FLOAT8(float8_div(fabs(float8_mi(l1->C, float8_mul(ratio, l2->C))),
                                HYPOT(l1->A, l1->B)));
}
```