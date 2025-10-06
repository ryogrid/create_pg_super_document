# percentile_cont_final_common

## Location
[src/backend/utils/adt/orderedsetaggs.c:526-612](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L526-L612)

## Overview
Core implementation function for the PERCENTILE_CONT ordered-set aggregate, providing common logic for continuous percentile calculation across different data types.

## Definition
```c
static Datum percentile_cont_final_common(FunctionCallInfo fcinfo,
                                         Oid expect_type,
                                         LerpFunc lerpfunc)
```

## Detailed Description
This static function implements the common logic for PostgreSQL's PERCENTILE_CONT aggregate function, which computes continuous percentiles by interpolating between adjacent values in a sorted dataset. Unlike PERCENTILE_DISC which returns an actual value from the dataset, PERCENTILE_CONT may return an interpolated value.

The function performs the following key operations:
1. Validates the percentile parameter (must be between 0 and 1)
2. Handles edge cases (NULL inputs, empty datasets)
3. Ensures the data is properly sorted using tuplesort
4. Calculates the exact position in the sorted data corresponding to the percentile
5. Retrieves the bounding values (first_row and second_row)
6. Either returns the exact value (if percentile falls on a data point) or calls the appropriate interpolation function

The percentile calculation uses the formula: `position = percentile * (n-1)` where n is the number of rows, then interpolates between floor(position) and ceil(position).

## Parameters / Member Variables
- `fcinfo`: Standard PostgreSQL function call information structure
- `expect_type`: Expected OID type for validation against the sorted column type
- `lerpfunc`: Type-specific linear interpolation function (e.g., float8_lerp, interval_lerp)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (validates aggregate context)
  - PG_GETARG_FLOAT8 (extracts percentile parameter)
  - [tuplesort_performsort](../t/tuplesort_performsort.md) (performs the sorting operation)
  - [tuplesort_rescan](../t/tuplesort_rescan.md) (resets sort state for re-reading)
  - [tuplesort_skiptuples](../t/tuplesort_skiptuples.md) (skips to the target row position)
  - [tuplesort_getdatum](../t/tuplesort_getdatum.md) (retrieves sorted values)
  - isnan (checks for NaN percentile values)
- Called from (representative examples):
  - [percentile_cont_float8_final](percentile_cont_float8_final.md)
  - [percentile_cont_interval_final](percentile_cont_interval_final.md)

## Notes and Other Information
- This is a static helper function that centralizes the complex logic of continuous percentile calculation
- Supports multiple data types through the LerpFunc parameter, allowing type-specific interpolation
- Handles edge cases robustly: NULL inputs, empty datasets, invalid percentile values
- Uses PostgreSQL's tuplesort infrastructure for efficient sorting and data retrieval
- The interpolation only occurs when the exact percentile position falls between two data points
- Part of PostgreSQL's implementation of SQL standard ordered-set aggregate functions
- Error handling includes validation of percentile range and proper error reporting for missing data

## Simplified Source

```c
static Datum
percentile_cont_final_common(FunctionCallInfo fcinfo,
                           Oid expect_type,
                           LerpFunc lerpfunc)
{
    OSAPerGroupState *osastate;
    double percentile;
    int64 first_row, second_row;
    Datum first_val, second_val, val;
    double proportion;
    bool isnull;

    // Validate percentile argument
    if (PG_ARGISNULL(1))
        PG_RETURN_NULL();

    percentile = PG_GETARG_FLOAT8(1);

    if (percentile < 0 || percentile > 1 || isnan(percentile))
        ereport(ERROR, "percentile value must be between 0 and 1");

    // Handle empty input
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

    if (osastate->number_of_rows == 0)
        PG_RETURN_NULL();

    // Ensure data is sorted
    if (!osastate->sort_done) {
        tuplesort_performsort(osastate->sortstate);
        osastate->sort_done = true;
    } else {
        tuplesort_rescan(osastate->sortstate);
    }

    // Calculate interpolation bounds: position = percentile * (n-1)
    first_row = floor(percentile * (osastate->number_of_rows - 1));
    second_row = ceil(percentile * (osastate->number_of_rows - 1));

    // Get first value
    tuplesort_skiptuples(osastate->sortstate, first_row, true);
    tuplesort_getdatum(osastate->sortstate, true, true, &first_val, &isnull, NULL);

    if (isnull)
        PG_RETURN_NULL();

    if (first_row == second_row) {
        // Exact match - no interpolation needed
        val = first_val;
    } else {
        // Get second value and interpolate
        tuplesort_getdatum(osastate->sortstate, true, true, &second_val, &isnull, NULL);

        if (isnull)
            PG_RETURN_NULL();

        // Calculate interpolation proportion
        proportion = (percentile * (osastate->number_of_rows - 1)) - first_row;
        val = lerpfunc(first_val, second_val, proportion);
    }

    PG_RETURN_DATUM(val);
}
```