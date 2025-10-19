# float8_avg

## Location
[src/backend/utils/adt/float.c:3118-3137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3118-L3137)

## Overview
Final function for the AVG aggregate that computes the arithmetic mean from accumulated transition state values maintained by accumulator functions.

## Definition

```c
Datum
float8_avg(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the final function for PostgreSQL's AVG aggregate when operating on floating-point data. It takes the transition state array produced by accumulator functions (like  or ) and computes the final average by dividing the sum (Sx) by the count (N).

The function implements SQL standard behavior by returning NULL when no input values were provided (N == 0), which is the correct result for AVG over an empty set according to SQL specifications. For non-empty sets, it returns the arithmetic mean as Sx/N.

The function expects a 3-element transition array but only uses the first two elements: the count and sum. The third element (sum of squared deviations) is ignored as it's not needed for average calculation.

## Parameters / Member Variables
-  (ArrayType*): Transition state array containing [N, Sx, Sxx] where only N (count) and Sx (sum) are used for average calculation

## Dependencies
- Functions called/Symbols referenced:
  - : Validates and extracts float8 values from the transition array
  - : PostgreSQL macro to get array argument
  - : PostgreSQL macro to return NULL value
  - : PostgreSQL macro to return float8 value

- Called from (representative examples):
  - AVG aggregate functions operating on floating-point columns
  - Statistical computations requiring arithmetic mean

## Notes and Other Information
- Returns NULL for empty input sets per SQL standard
- Only uses first two elements of 3-element transition array (N and Sx)
- Simple division operation: result = sum / count
- Part of PostgreSQL's aggregate function infrastructure
- Located in src/backend/utils/adt/float.c:3118-3137
- Handles the final step of AVG aggregate computation after accumulation phase

## Simplified Source

```c
Datum
float8_avg(PG_FUNCTION_ARGS)
{
    ArrayType *transarray = PG_GETARG_ARRAYTYPE_P(0);

    // Extract N (count) and Sx (sum) from transition state
    float8 *transvalues = check_float8_array(transarray, "float8_avg", 3);
    float8 N = transvalues[0];   // Count of values
    float8 Sx = transvalues[1];  // Sum of values
    // transvalues[2] (Sxx) ignored for average calculation

    // SQL standard: AVG of empty set returns NULL
    if (N == 0.0)
        PG_RETURN_NULL();

    // Return arithmetic mean: sum / count
    PG_RETURN_FLOAT8(Sx / N);
}
```