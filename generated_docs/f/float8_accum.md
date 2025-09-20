# float8_accum

## Location
[src/backend/utils/adt/float.c:2950-3032](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2950-L3032)

## Overview
PostgreSQL aggregate transition function that accumulates statistical data for floating-point values using the numerically stable Youngs-Cramer algorithm.

## Definition

```c
struct a
	 * new array with the updated transition data and return it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		transvalues[0] = N;
		transvalues[1] = Sx;
		transvalues[2] = Sxx;

		PG_RETURN_ARRAYTYPE_P(transarray);
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
float4_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
```
## Detailed Description
The `float8_accum` function is the core transition function for PostgreSQL's floating-point statistical aggregates including AVG(), VAR_SAMP(), VAR_POP(), STDDEV_SAMP(), and STDDEV_POP(). It accumulates each new float8 value into a 3-element statistical state array [N, Sx, Sxx] using the Youngs-Cramer algorithm, which provides superior numerical stability compared to the naive sum(X) and sum(X²) approach.

The function updates the statistical accumulators as follows:
- N (count) is incremented by 1
- Sx (sum) is updated by adding the new value
- Sxx (sum of squared deviations) is updated using the Youngs-Cramer formula: Sxx += (newval * N - Sx)² / (N * (N-1))

The algorithm includes comprehensive error handling for overflow conditions and special values (NaN, infinity), ensuring robust behavior across all input ranges.

## Parameters
- `transarray`: ArrayType containing current 3-element statistical state [N, Sx, Sxx]
- `newval`: New float8 value to accumulate into the statistical state

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P (macro to extract ArrayType argument)
  - PG_GETARG_FLOAT8 (macro to extract float8 argument)
  - [check_float8_array](../c/check_float8_array.md) (validation helper function)
  - isinf (check for infinite values)
  - isnan (check for NaN values)
  - [float_overflow_error](float_overflow_error.md) (PostgreSQL error handling)
  - get_float8_nan (get NaN float8 value)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (check if called in aggregate context)
  - Float8GetDatumFast (convert float8 to Datum)
  - [construct_array](../c/construct_array.md) (create new ArrayType)
  - PG_RETURN_ARRAYTYPE_P (macro to return ArrayType result)
- Called from (representative examples):
  - No direct references found in the codebase (used by aggregate system)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:2950-3032
- Implements the Youngs-Cramer algorithm for numerically stable variance computation
- Handles special cases for first input (N=0) and infinite/NaN inputs appropriately
- Optimizes memory usage by modifying input array in-place when called as aggregate
- Critical component of PostgreSQL's statistical aggregate infrastructure
- Provides foundation for computing accurate means, variances, and standard deviations
- Prevents common numerical instability issues in statistical computations
- Ensures Sxx becomes NaN when inputs contain infinite or NaN values to maintain mathematical correctness
- Part of the sophisticated error handling system that distinguishes between overflow from finite inputs vs. propagation of infinite/NaN inputs