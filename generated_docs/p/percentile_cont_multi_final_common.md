# percentile_cont_multi_final_common

## Location
[src/backend/utils/adt/orderedsetaggs.c:848-1003](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L848-L1003)

## Overview
A static helper function that implements the common logic for computing continuous percentiles across multiple data types using linear interpolation between adjacent values.

## Definition
```c
static Datum percentile_cont_multi_final_common(FunctionCallInfo fcinfo,
                                               Oid expect_type,
                                               int16 typLen, bool typByVal, char typAlign,
                                               LerpFunc lerpfunc)
```

## Detailed Description  
This function serves as the common implementation for all continuous percentile aggregate functions that operate on arrays of percentile values. Unlike discrete percentiles which return exact row values, continuous percentiles use linear interpolation between adjacent rows to compute fractional positions.

The function handles the complex logic of managing row positions, fetching appropriate data values, and performing interpolation calculations. It optimally reuses previously fetched values when multiple percentiles require interpolation between the same pair of rows, minimizing data access operations.

Key operations include processing NULL values in percentile arrays, sorting required row positions for efficient access, performing tuplesort operations to fetch specific rows, and applying type-specific interpolation through the provided LerpFunc callback.

## Parameters / Member Variables
- `fcinfo`: Standard PostgreSQL function call information structure
- `expect_type`: Expected OID of the data type being processed
- `typLen`: Length of the data type (-1 for variable length types)
- `typByVal`: Whether the data type is passed by value (true) or reference (false) 
- `typAlign`: Alignment requirement for the data type ('c', 's', 'i', 'd')
- `lerpfunc`: Linear interpolation function pointer for the specific data type

## Dependencies
- Functions called/Symbols referenced:
  - [OSAPerGroupState](../O/OSAPerGroupState.md) (struct type)
  - [pct_info](pct_info.md) (struct type) 
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (struct type)
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
  - [percentile_cont_float8_multi_final](percentile_cont_float8_multi_final.md)
  - [percentile_cont_interval_multi_final](percentile_cont_interval_multi_final.md)

## Notes and Other Information
- This is a static function providing shared implementation for type-specific percentile functions
- Uses linear interpolation via type-specific LerpFunc to compute values between adjacent rows
- Optimizes performance by reusing fetched values when consecutive percentiles interpolate between same row pairs
- Handles edge cases where first_row equals second_row (no interpolation needed)
- Maintains strict type safety by validating expected data type matches aggregate state
- Memory management uses palloc for result arrays within PostgreSQL's memory context system
- Part of PostgreSQL's ordered-set aggregate framework enabling statistical functions
- Critical for implementing SQL standard percentile_cont aggregates with proper continuous semantics

## Simplified Source

```c
static Datum
percentile_cont_multi_final_common(FunctionCallInfo fcinfo,
                                   Oid expect_type,
                                   int16 typLen, bool typByVal, char typAlign,
                                   LerpFunc lerpfunc)
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
    Datum first_val = (Datum) 0;
    Datum second_val = (Datum) 0;
    bool isnull;
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

    // Setup percentile calculation info (continuous mode)
    pct_info = setup_pct_info(num_percentiles,
                              percentiles_datum,
                              percentiles_null,
                              osastate->number_of_rows,
                              true);

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

    // Process non-NULL percentiles with interpolation
    if (i < num_percentiles) {
        // Ensure data is sorted
        if (!osastate->sort_done) {
            tuplesort_performsort(osastate->sortstate);
            osastate->sort_done = true;
        } else {
            tuplesort_rescan(osastate->sortstate);
        }

        for (; i < num_percentiles; i++) {
            int64 first_row = pct_info[i].first_row;
            int64 second_row = pct_info[i].second_row;
            int idx = pct_info[i].idx;

            // Advance to first_row if needed
            if (first_row > rownum) {
                tuplesort_skiptuples(osastate->sortstate, first_row - rownum - 1, true);
                tuplesort_getdatum(osastate->sortstate, true, true, &first_val, &isnull, NULL);
                rownum = first_row;
                second_val = first_val;
            } else if (first_row == rownum) {
                first_val = second_val;
            }

            // Fetch second_row if needed
            if (second_row > rownum) {
                tuplesort_getdatum(osastate->sortstate, true, true, &second_val, &isnull, NULL);
                rownum++;
            }

            // Compute result: interpolate or use exact value
            if (second_row > first_row)
                result_datum[idx] = lerpfunc(first_val, second_val, pct_info[i].proportion);
            else
                result_datum[idx] = first_val;

            result_isnull[idx] = false;
        }
    }

    // Return array with same shape as input
    PG_RETURN_POINTER(construct_md_array(result_datum, result_isnull,
                                         ARR_NDIM(param),
                                         ARR_DIMS(param), ARR_LBOUND(param),
                                         expect_type,
                                         typLen,
                                         typByVal,
                                         typAlign));
}
```