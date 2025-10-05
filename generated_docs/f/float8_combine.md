# float8_combine

## Location
[src/backend/utils/adt/float.c:2856-2949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2856-L2949)

## Overview
PostgreSQL aggregate combine function that merges two sets of statistical transition data for parallel execution of floating-point statistical aggregates.

## Definition

```c
struct a
	 * new array with the updated transition data and return it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		transvalues1[0] = N;
		transvalues1[1] = Sx;
		transvalues1[2] = Sxx;

		PG_RETURN_ARRAYTYPE_P(transarray1);
	}
	else
	{
		Datum		transdatums[3];
		ArrayType  *result;

		transdatums[0] = Float8GetDatumFast(N);
		transdatums[1] = Float8GetDatumFast(Sx);
		transdatums[2] = Float8GetDatumFast(Sxx);

		result = construct_array(transdatums, 3,
								 FLOAT8OID,
								 sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);

		PG_RETURN_ARRAYTYPE_P(result);
	}
}

Datum
float8_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
```
## Detailed Description
The `float8_combine` function is a specialized aggregate combine function designed for PostgreSQL's two-stage aggregation system. It takes two 3-element float8 arrays representing statistical transition data (N, Sx, Sxx) and combines them into a single transition data array using a generalized Youngs-Cramer algorithm.

This function is essential for parallel query execution, allowing statistical aggregates like AVG(), VAR_SAMP(), VAR_POP(), STDDEV_SAMP(), and STDDEV_POP() to be computed across multiple worker processes and then combined efficiently. The function handles special cases where one or both input arrays represent empty sets (N=0) and includes overflow detection for numerical stability.

The combination algorithm preserves the mathematical properties required for accurate final aggregate computation:
- N = N1 + N2
- Sx = Sx1 + Sx2  
- Sxx = Sxx1 + Sxx2 + N1 * N2 * (Sx1/N1 - Sx2/N2)^2 / N

## Parameters
- `transarray1`: First ArrayType containing 3-element float8 array [N1, Sx1, Sxx1]
- `transarray2`: Second ArrayType containing 3-element float8 array [N2, Sx2, Sxx2]

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P (macro to extract ArrayType arguments)
  - [check_float8_array](../c/check_float8_array.md) (validation helper function)
  - [float8_pl](float8_pl.md) (safe float8 addition)
  - isinf (check for infinite results)
  - [float_overflow_error](float_overflow_error.md) (PostgreSQL error handling)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (check if called in aggregate context)
  - Float8GetDatumFast (convert float8 to Datum)
  - [construct_array](../c/construct_array.md) (create new ArrayType)
  - PG_RETURN_ARRAYTYPE_P (macro to return ArrayType result)
- Called from (representative examples):
  - No direct references found in the codebase (used by aggregate system)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:2856-2949
- Used exclusively in two-stage aggregation for parallel query processing
- Should not be called outside of aggregate context
- Optimizes memory usage by modifying input array in-place when called as aggregate
- Handles division-by-zero cases by treating empty sets specially
- Part of PostgreSQL's sophisticated parallel aggregation infrastructure
- Implements mathematically sound combination preserving numerical accuracy
- Supports the Youngs-Cramer algorithm for numerically stable statistical computation

## Simplified Source

```c
Datum float8_combine(PG_FUNCTION_ARGS) {
    // Get two transition arrays [N, Sx, Sxx]
    ArrayType *transarray1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *transarray2 = PG_GETARG_ARRAYTYPE_P(1);

    // Validate and extract values
    float8 *transvalues1 = check_float8_array(transarray1, "float8_combine", 3);
    float8 *transvalues2 = check_float8_array(transarray2, "float8_combine", 3);

    float8 N1 = transvalues1[0], Sx1 = transvalues1[1], Sxx1 = transvalues1[2];
    float8 N2 = transvalues2[0], Sx2 = transvalues2[1], Sxx2 = transvalues2[2];

    // Combine using Youngs-Cramer algorithm
    float8 N, Sx, Sxx;
    if (N1 == 0.0) {
        // First array is empty, use second
        N = N2; Sx = Sx2; Sxx = Sxx2;
    } else if (N2 == 0.0) {
        // Second array is empty, use first
        N = N1; Sx = Sx1; Sxx = Sxx1;
    } else {
        // Combine both arrays
        N = N1 + N2;
        Sx = float8_pl(Sx1, Sx2);
        float8 tmp = Sx1 / N1 - Sx2 / N2;
        Sxx = Sxx1 + Sxx2 + N1 * N2 * tmp * tmp / N;

        // Check for overflow
        if (unlikely(isinf(Sxx)) && !isinf(Sxx1) && !isinf(Sxx2)) {
            float_overflow_error();
        }
    }

    // Return result (optimize by modifying in-place if in aggregate context)
    if (AggCheckCallContext(fcinfo, NULL)) {
        transvalues1[0] = N; transvalues1[1] = Sx; transvalues1[2] = Sxx;
        PG_RETURN_ARRAYTYPE_P(transarray1);
    } else {
        Datum transdatums[3] = {
            Float8GetDatumFast(N),
            Float8GetDatumFast(Sx),
            Float8GetDatumFast(Sxx)
        };
        ArrayType *result = construct_array(transdatums, 3, FLOAT8OID,
                                          sizeof(float8), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);
        PG_RETURN_ARRAYTYPE_P(result);
    }
}
```