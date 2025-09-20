# check_float8_array

## Location
[src/backend/utils/adt/float.c:2832-2855](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2832-L2855)

## Overview
Internal helper function that validates and extracts data from a PostgreSQL array expected to contain float8 elements for statistical aggregate functions.

## Definition

```c
struct_array() since the array data is just
	 * going to look like a C array of N float8 values.
	 */
	if (ARR_NDIM(transarray) != 1 ||
		ARR_DIMS(transarray)[0] != n ||
		ARR_HASNULL(transarray) ||
		ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "%s: expected %d-element float8 array", caller, n);
```
## Detailed Description
The `check_float8_array` function is a utility function used extensively throughout PostgreSQL's floating-point aggregate operators. It validates that an input ArrayType pointer represents a properly formatted N-element array of float8 values and returns a direct pointer to the array data. This function is critical for the implementation of statistical aggregates like AVG(), VAR_SAMP(), VAR_POP(), STDDEV_SAMP(), STDDEV_POP(), and various regression functions.

The function performs comprehensive validation to ensure the array meets all requirements:
- Must be one-dimensional
- Must contain exactly N elements
- Must not contain any NULL values  
- Must have element type FLOAT8OID

If any validation fails, the function throws an error with context about which calling function detected the problem.

## Parameters / Member Variables
- `transarray`: Pointer to the ArrayType structure to be validated and processed
- `caller`: String identifying the calling function, used in error messages for debugging
- `n`: Expected number of elements the array should contain

## Dependencies
- Functions called/Symbols referenced:
  - ARR_NDIM (macro to get array dimensions)
  - ARR_DIMS (macro to get array dimension sizes)
  - ARR_HASNULL (macro to check for NULL elements)
  - ARR_ELEMTYPE (macro to get array element type)
  - ARR_DATA_PTR (macro to get pointer to array data)
  - elog (PostgreSQL logging/error function)
- Called from (representative examples):
  - [float8_combine](../f/float8_combine.md) (multiple calls)
  - [float8_accum](../f/float8_accum.md)
  - [float4_accum](../f/float4_accum.md)
  - [float8_avg](../f/float8_avg.md)
  - [float8_var_pop](../f/float8_var_pop.md)
  - [float8_var_samp](../f/float8_var_samp.md)
  - [float8_stddev_pop](../f/float8_stddev_pop.md)
  - [float8_stddev_samp](../f/float8_stddev_samp.md)
  - Various regression and covariance functions

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:2832-2855
- Static function - only accessible within the float.c compilation unit
- Part of PostgreSQL's sophisticated numerical aggregate system using the Youngs-Cramer algorithm
- Provides type safety and validation for statistical computations
- Returns direct pointer to array data, avoiding unnecessary data copying
- Used extensively in PostgreSQL's statistical and mathematical aggregate functions
- Critical for maintaining data integrity in complex statistical calculations