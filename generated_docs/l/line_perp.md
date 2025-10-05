# line_perp

## Location
[src/backend/utils/adt/geo_ops.c:1155-1173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1155-L1173)

## Overview
Determines whether two LINE objects are perpendicular to each other in PostgreSQL's geometric data type system.

## Definition

```c
Datum
line_perp(PG_FUNCTION_ARGS)
```
## Detailed Description
This function checks if two LINE objects are perpendicular by analyzing the relationship between their coefficients. In the standard line equation Ax + By + C = 0, two lines are perpendicular if the dot product of their direction vectors is zero. The function handles several special cases for vertical and horizontal lines, then performs the general perpendicularity test by checking if (A1*A2)/(B1*B2) equals -1, which indicates that the slopes are negative reciprocals of each other.

## Parameters / Member Variables
- : PostgreSQL function argument macro that expands to access two LINE parameters:
  - First line (l1): First line for perpendicular test  
  - Second line (l2): Second line for perpendicular test

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LINE_P: Extracts LINE arguments from function call
  - FPzero: Tests if a floating-point value is zero
  - [FPeq](../F/FPeq.md): Tests if two floating-point values are equal
  - [float8_mul](../f/float8_mul.md): Multiplies two float8 values
  - [float8_div](../f/float8_div.md): Divides two float8 values  
  - PG_RETURN_BOOL: Returns boolean result
- Called from (representative examples):
  - No direct callers found (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1155-1173
- Part of PostgreSQL's relative position routines for geometric operations
- Handles special cases for vertical (A=0) and horizontal (B=0) lines
- Uses floating-point arithmetic with appropriate precision handling via FP macros
- The function follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS macro
- Returns true if lines are perpendicular, false otherwise
- The perpendicularity test is based on the mathematical property that perpendicular lines have slopes that are negative reciprocals

## Simplified Source

```c
Datum
line_perp(PG_FUNCTION_ARGS)
{
    LINE *l1 = PG_GETARG_LINE_P(0);
    LINE *l2 = PG_GETARG_LINE_P(1);

    // Handle special cases for vertical/horizontal lines
    if (FPzero(l1->A))          // l1 is horizontal
        PG_RETURN_BOOL(FPzero(l2->B));  // l2 must be vertical
    if (FPzero(l2->A))          // l2 is horizontal
        PG_RETURN_BOOL(FPzero(l1->B));  // l1 must be vertical
    if (FPzero(l1->B))          // l1 is vertical
        PG_RETURN_BOOL(FPzero(l2->A));  // l2 must be horizontal
    if (FPzero(l2->B))          // l2 is vertical
        PG_RETURN_BOOL(FPzero(l1->A));  // l1 must be horizontal

    // General case: check if (A1*A2)/(B1*B2) == -1
    PG_RETURN_BOOL(FPeq(float8_div(float8_mul(l1->A, l2->A),
                                   float8_mul(l1->B, l2->B)), -1.0));
}
```