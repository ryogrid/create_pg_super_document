# float8_stddev_pop

## Location
[src/backend/utils/adt/float.c:3182-3203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3182-L3203)

## Overview
Final function for the STDDEV_POP aggregate that computes the population standard deviation from accumulated transition state values.

## Definition

```c
Datum
float8_stddev_pop(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the final function for PostgreSQL's STDDEV_POP (population standard deviation) aggregate when operating on floating-point data. It computes the population standard deviation by taking the square root of the population variance, which is calculated as the sum of squared deviations (Sxx) divided by the count (N).

Population standard deviation uses the formula: σ = √(Σ(xi - μ)² / N), where N is the total number of values. This provides the standard deviation for the entire population, as opposed to sample standard deviation which divides by (N-1). The function leverages the numerically stable sum of squared deviations maintained by accumulator functions through the Youngs-Cramer algorithm.

The function returns NULL for empty input sets (N == 0) since population standard deviation is mathematically undefined for zero elements. For non-empty sets, it computes sqrt(Sxx/N), where the square root of the guaranteed non-negative variance yields the standard deviation.

## Parameters / Member Variables
-  (ArrayType*): Transition state array containing [N, Sx, Sxx] where N (count) and Sxx (sum of squared deviations) are used for standard deviation calculation

## Dependencies
- Functions called/Symbols referenced:
  - : Validates and extracts float8 values from the transition array
  - : Mathematical square root function from standard math library
  - : PostgreSQL macro to get array argument
  - : PostgreSQL macro to return NULL value
  - : PostgreSQL macro to return float8 value

- Called from (representative examples):
  - STDDEV_POP aggregate functions operating on floating-point columns
  - Statistical computations requiring population standard deviation

## Notes and Other Information
- Returns NULL for empty input sets since population standard deviation is undefined for N=0
- Uses first and third elements of transition array (N and Sxx), ignores Sx (sum)
- Computes square root of population variance: sqrt(Sxx/N)
- [Result](../R/Result.md) is guaranteed to be non-negative due to mathematical properties of standard deviation
- Population standard deviation divides by N, unlike sample standard deviation which divides by (N-1)
- Part of PostgreSQL's statistical aggregate infrastructure
- Located in src/backend/utils/adt/float.c:3182-3203
- Relies on numerically stable Sxx values computed by accumulator functions

## Simplified Source

```c
Datum
float8_stddev_pop(PG_FUNCTION_ARGS)
{
    ArrayType *transarray = PG_GETARG_ARRAYTYPE_P(0);

    // Extract N (count) and Sxx (sum of squared deviations) from transition state
    float8 *transvalues = check_float8_array(transarray, "float8_stddev_pop", 3);
    float8 N = transvalues[0];    // Count of values
    // transvalues[1] (Sx) ignored for standard deviation calculation
    float8 Sxx = transvalues[2];  // Sum of squared deviations

    // Population standard deviation undefined for empty set
    if (N == 0.0)
        PG_RETURN_NULL();

    // Return population standard deviation: sqrt(Sxx / N)
    PG_RETURN_FLOAT8(sqrt(Sxx / N));
}
```