# percentile_disc_multi_final

## Location
[src/backend/utils/adt/orderedsetaggs.c:731-847](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L731-L847)

## Overview
The final aggregate function that computes discrete percentiles for an array of percentile values using ordered set semantics.

## Definition
```c
Datum percentile_disc_multi_final(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the final phase of the percentile_disc aggregate function when multiple percentile values are requested simultaneously. It processes a sorted dataset and returns exact values from specific row positions that correspond to each requested percentile.

Unlike continuous percentiles which interpolate between values, discrete percentiles return the actual value at the computed row position. The function efficiently handles multiple percentiles by sorting the required row positions and scanning through the dataset only once.

The function handles various edge cases including NULL input values, empty datasets, NULL percentile values in the input array, and missing rows. It maintains the same array structure as the input percentile array for the output.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: OSAPerGroupState pointer (aggregate state)  
  - Argument 1: ArrayType pointer (array of percentile values)

## Dependencies
- Functions called/Symbols referenced:
  - [OSAPerGroupState](../O/OSAPerGroupState.md) (struct type)
  - [pct_info](pct_info.md) (struct type)
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - PG_GETARG_ARRAYTYPE_P
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - [construct_empty_array](../c/construct_empty_array.md)
  - [setup_pct_info](../s/setup_pct_info.md)
  - [tuplesort_performsort](../t/tuplesort_performsort.md)
  - [tuplesort_rescan](../t/tuplesort_rescan.md)
  - [tuplesort_skiptuples](../t/tuplesort_skiptuples.md)  
  - [tuplesort_getdatum](../t/tuplesort_getdatum.md)
  - [construct_md_array](../c/construct_md_array.md)
  - ARR_NDIM/ARR_DIMS/ARR_LBOUND (array macros)
  - [palloc](palloc.md) (memory allocation)
- Called from (representative examples):
  - PostgreSQL aggregate execution framework (no direct code references found)

## Notes and Other Information
- This is a PostgreSQL aggregate final function, called automatically by the executor during aggregate processing
- Uses tuplesort for efficient access to sorted data, avoiding full dataset materialization
- Optimizes performance by processing multiple percentiles in a single scan when possible
- Returns NULL if no input rows exist or if the percentile array argument is NULL
- Preserves the dimensionality and bounds of the input percentile array in the output
- Error handling includes checks for missing rows that should be present based on row count
- Memory management uses palloc for result arrays which will be cleaned up by PostgreSQL's memory context system
- Part of PostgreSQL's ordered-set aggregate framework for statistical functions

## Simplified Source

```c
Datum
percentile_disc_multi_final(PG_FUNCTION_ARGS)
{
    OSAPerGroupState *osastate;
    ArrayType *param;
    Datum *percentiles_datum;
    bool *percentiles_null;
    int num_percentiles;
    struct pct_info *pct_info;
    Datum *result_datum;
    bool *result_isnull;
    int64 rownum = 0;
    Datum val = (Datum) 0;
    bool isnull = true;
    int i;

    // Handle empty input
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    osastate = (OSAPerGroupState *) PG_GETARG_POINTER(0);

    if (osastate->number_of_rows == 0)
        PG_RETURN_NULL();

    // Parse percentile array input
    if (PG_ARGISNULL(1))
        PG_RETURN_NULL();
    param = PG_GETARG_ARRAYTYPE_P(1);

    deconstruct_array_builtin(param, FLOAT8OID,
                              &percentiles_datum,
                              &percentiles_null,
                              &num_percentiles);

    if (num_percentiles == 0)
        PG_RETURN_POINTER(construct_empty_array(osastate->qstate->sortColType));

    // Setup percentile calculation info (discrete mode)
    pct_info = setup_pct_info(num_percentiles,
                              percentiles_datum,
                              percentiles_null,
                              osastate->number_of_rows,
                              false);

    // Allocate result arrays
    result_datum = (Datum *) palloc(num_percentiles * sizeof(Datum));
    result_isnull = (bool *) palloc(num_percentiles * sizeof(bool));

    // Handle NULL percentiles first
    for (i = 0; i < num_percentiles; i++) {
        int idx = pct_info[i].idx;

        if (pct_info[i].first_row > 0)
            break;

        result_datum[idx] = (Datum) 0;
        result_isnull[idx] = true;
    }

    // Process non-NULL percentiles
    if (i < num_percentiles) {
        // Ensure data is sorted
        if (!osastate->sort_done) {
            tuplesort_performsort(osastate->sortstate);
            osastate->sort_done = true;
        } else {
            tuplesort_rescan(osastate->sortstate);
        }

        // Extract values for each percentile
        for (; i < num_percentiles; i++) {
            int64 target_row = pct_info[i].first_row;
            int idx = pct_info[i].idx;

            // Advance to target row
            if (target_row > rownum) {
                tuplesort_skiptuples(osastate->sortstate, target_row - rownum - 1, true);
                tuplesort_getdatum(osastate->sortstate, true, true, &val, &isnull, NULL);
                rownum = target_row;
            }

            result_datum[idx] = val;
            result_isnull[idx] = isnull;
        }
    }

    // Return array with same shape as input
    PG_RETURN_POINTER(construct_md_array(result_datum, result_isnull,
                                         ARR_NDIM(param),
                                         ARR_DIMS(param),
                                         ARR_LBOUND(param),
                                         osastate->qstate->sortColType,
                                         osastate->qstate->typLen,
                                         osastate->qstate->typByVal,
                                         osastate->qstate->typAlign));
}
```