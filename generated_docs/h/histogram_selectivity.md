# histogram_selectivity

## Location
[src/backend/utils/adt/selfuncs.c:824-914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L824-L914)

## Overview
Examines histogram statistics to estimate selectivity for predicates by treating histogram entries as a representative sample of column values.

## Definition
```c
double
histogram_selectivity(VariableStatData *vardata,
                      FmgrInfo *opproc, Oid collation,
                      Datum constval, bool varonleft,
                      int min_hist_size, int n_skip,
                      int *hist_size)
```

## Detailed Description
`histogram_selectivity` provides selectivity estimation by using the histogram as a representative sample of column data distribution. The function:

1. **Histogram Access**: Retrieves histogram data from column statistics
2. **Sample Testing**: Tests each histogram entry against the predicate condition
3. **Outlier Handling**: Optionally skips outlier entries at the beginning and end of the histogram
4. **Size Validation**: Ensures the histogram is large enough to provide meaningful estimates
5. **Selectivity Calculation**: Computes the fraction of matching entries as the selectivity estimate

The approach is generic and works with any boolean-returning predicate operator, not just those related to the histogram sort operator. However, it requires sufficiently large histograms to be representative.

## Parameters / Member Variables
- `vardata`: Statistical data structure containing column histogram
- `opproc`: Function manager info for the comparison operator to be applied
- `collation`: Collation OID for string comparison operations
- `constval`: The constant value being compared against histogram entries
- `varonleft`: Boolean indicating whether variable is on left side of operator
- `min_hist_size`: Minimum required histogram size for meaningful results
- `n_skip`: Number of outlier entries to skip at each end of histogram
- `hist_size`: Output parameter receiving the actual histogram size

## Dependencies
- Functions called/Symbols referenced:
  - [statistic_proc_security_check](../s/statistic_proc_security_check.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - STATISTIC_KIND_HISTOGRAM
  - ATTSTATSSLOT_VALUES
- Called from (representative examples):
  - [generic_restriction_selectivity](../g/generic_restriction_selectivity.md)
  - [patternsel_common](../p/patternsel_common.md)

## Notes and Other Information
- Returns -1 if no histogram exists or histogram is smaller than min_hist_size
- Typical parameter values: min_hist_size=10, n_skip=1
- [Result](../R/Result.md) excludes most-common-values and null entries - caller must combine with other statistics
- Uses manual function invocation to handle potential NULL results gracefully
- Outlier skipping helps improve representativeness by removing extreme values
- Security check ensures operator function is safe to execute with column statistics
- Caller should be prepared for fallback estimation when histogram is unavailable or too small

## Simplified Source

```c
double histogram_selectivity(VariableStatData *vardata,
                            FmgrInfo *opproc, Oid collation,
                            Datum constval, bool varonleft,
                            int min_hist_size, int n_skip,
                            int *hist_size) {
    // Validate parameters
    Assert(n_skip >= 0);
    Assert(min_hist_size > 2 * n_skip);

    // Try to get histogram from column statistics
    AttStatsSlot sslot;
    if (HeapTupleIsValid(vardata->statsTuple) &&
        statistic_proc_security_check(vardata, opproc->fn_oid) &&
        get_attstatsslot(&sslot, vardata->statsTuple,
                        STATISTIC_KIND_HISTOGRAM, InvalidOid,
                        ATTSTATSSLOT_VALUES)) {

        *hist_size = sslot.nvalues;

        // Only proceed if histogram is large enough
        if (sslot.nvalues >= min_hist_size) {
            // Set up function call for operator
            LOCAL_FCINFO(fcinfo, 2);
            InitFunctionCallInfoData(*fcinfo, opproc, 2, collation, NULL, NULL);
            fcinfo->args[0].isnull = false;
            fcinfo->args[1].isnull = false;

            // Set constant value based on operator direction
            if (varonleft)
                fcinfo->args[1].value = constval;
            else
                fcinfo->args[0].value = constval;

            // Test each histogram entry against the predicate
            int nmatch = 0;
            for (int i = n_skip; i < sslot.nvalues - n_skip; i++) {
                // Set histogram value based on operator direction
                if (varonleft)
                    fcinfo->args[0].value = sslot.values[i];
                else
                    fcinfo->args[1].value = sslot.values[i];

                // Execute operator and count matches
                fcinfo->isnull = false;
                Datum result = FunctionCallInvoke(fcinfo);
                if (!fcinfo->isnull && DatumGetBool(result))
                    nmatch++;
            }

            // Calculate selectivity as fraction of matches
            double selectivity = ((double) nmatch) / ((double) (sslot.nvalues - 2 * n_skip));
            free_attstatsslot(&sslot);
            return selectivity;
        } else {
            free_attstatsslot(&sslot);
            return -1;  // Histogram too small
        }
    } else {
        *hist_size = 0;
        return -1;  // No histogram available
    }
}
```