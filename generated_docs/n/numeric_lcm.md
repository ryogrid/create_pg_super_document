# numeric_lcm

## Location
src/backend/utils/adt/numeric.c: 3580 - 3639

## Overview
Calculates the least common multiple (LCM) of two numeric values using the mathematical relationship LCM(x,y) = abs(x / GCD(x,y) * y).

## Definition
```c
Datum numeric_lcm(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_lcm` function implements the mathematical least common multiple operation for PostgreSQL numeric data types. It computes LCM using the standard mathematical formula: LCM(x, y) = abs(x / GCD(x, y) * y). The function handles special cases including NaN and infinity values by returning NaN, and zero inputs by returning zero. The implementation ensures that the division by GCD is exact (returning an integer), making the LCM an integral multiple of both inputs. The results display scale is set to the maximum of the input scales to preserve precision formatting.