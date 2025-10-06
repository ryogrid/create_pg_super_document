# percentile_disc_final

## Location
[src/backend/utils/adt/orderedsetaggs.c:427-502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L427-L502)

## Overview
Final function for the  ordered-set aggregate that calculates discrete percentiles by returning the first value whose position is at or above the specified percentile.

## Definition

```c
typedef Datum (*LerpFunc) (Datum lo, Datum hi, double pct);
```
## Detailed Description
The  function implements the final phase of the  aggregate. It calculates discrete percentiles, meaning it returns an actual value from the input set rather than interpolating between values (as  would).

The function first validates that the percentile argument is between 0 and 1. If there are no input rows or all input values were null, it returns null. Otherwise, it ensures the collected data is sorted and uses the mathematical formula  to determine which row to return, where N is the total number of non-null rows and K is the 1-based position of the desired row.

The function handles edge cases properly: for percentile 0, it returns the first value; for percentile 1, it returns the last value. The implementation skips K-1 rows and returns the Kth row, which corresponds to the smallest value whose cumulative distribution is greater than or equal to the requested percentile.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments macro containing:
  - Argument 0: OSAPerGroupState pointer from transition function (may be null if no input rows)
  - Argument 1: Percentile value (float8, must be between 0.0 and 1.0)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [tuplesort_performsort](../t/tuplesort_performsort.md)
  - [tuplesort_rescan](../t/tuplesort_rescan.md)
  - [tuplesort_skiptuples](../t/tuplesort_skiptuples.md)
  - [tuplesort_getdatum](../t/tuplesort_getdatum.md)
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_GETARG_FLOAT8
  - PG_RETURN_NULL
  - PG_RETURN_DATUM
  - isnan
  - ceil
  - ereport
- Called from (representative examples):
  - PostgreSQL aggregate execution framework as final function for percentile_disc aggregate

## Notes and Other Information
- Returns discrete percentiles (actual input values) rather than interpolated values
- Validates percentile parameter is in range [0.0, 1.0] and not NaN
- Handles empty result sets and all-null inputs by returning null
- Uses  formula to determine the position of the result row
- Supports rescanning if the aggregate state is shared across execution nodes
- Part of SQL standard ordered-set aggregate functions
- Commonly used for median calculations: percentile_disc(0.5)

## Simplified Source

```c
Datum
percentile_disc_final(PG_FUNCTION_ARGS)
{
    OSAPerGroupState *osastate;
    double percentile;
    Datum val;
    bool isnull;
    int64 rownum;

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

    // Calculate position: K = ceil(N * percentile)
    // Skip K-1 rows and return the Kth row
    rownum = (int64) ceil(percentile * osastate->number_of_rows);

    if (rownum > 1) {
        tuplesort_skiptuples(osastate->sortstate, rownum - 1, true);
    }

    // Get the result value
    tuplesort_getdatum(osastate->sortstate, true, true, &val, &isnull, NULL);

    return isnull ? PG_RETURN_NULL() : PG_RETURN_DATUM(val);
}
```