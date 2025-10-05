# ineq_histogram_selectivity

## Location
[src/backend/utils/adt/selfuncs.c:1042-1400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L1042-L1400)

## Overview
Specialized histogram analysis function for inequality operators that uses binary search and linear interpolation within histogram bins to provide precise selectivity estimates for scalar inequality conditions.

## Definition
```c
double
ineq_histogram_selectivity(PlannerInfo *root,
                           VariableStatData *vardata,
                           Oid opoid, FmgrInfo *opproc, bool isgt, bool iseq,
                           Oid collation,
                           Datum constval, Oid consttype)
```

## Detailed Description
`ineq_histogram_selectivity` is the core engine for inequality selectivity estimation in PostgreSQL. It implements a sophisticated algorithm that:

1. **Compatibility Checking**: Ensures the histogram sort operator is compatible with the query operator
2. **Binary Search**: Uses binary search to locate the histogram bin containing the comparison value
3. **Endpoint Refinement**: Attempts to get current min/max values when accessing histogram boundaries
4. **Linear Interpolation**: Performs linear interpolation within the identified bin using `convert_to_scalar`
5. **Equality Adjustment**: Applies corrections for inclusive vs exclusive inequality operators
6. **Fallback Brute-Force**: Uses direct comparison when histogram sort order is incompatible

The function handles edge cases like identical bin boundaries, conversion failures, and provides appropriate clamping of extreme estimates.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context
- `vardata`: Statistical data structure containing column histogram
- `opoid`: OID of the inequality operator being evaluated
- `opproc`: Function manager info for the comparison operator
- `isgt`: Boolean flag indicating if this is a "greater than" type operator (> or >=)
- `iseq`: Boolean flag indicating if equality is included (<= or >=)
- `collation`: Collation OID for string comparison operations
- `constval`: The constant value being compared against
- `consttype`: Data type OID of the constant value

## Dependencies
- Functions called/Symbols referenced:
  - [statistic_proc_security_check](../s/statistic_proc_security_check.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [comparison_ops_are_compatible](../c/comparison_ops_are_compatible.md)
  - [get_actual_variable_range](../g/get_actual_variable_range.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [convert_to_scalar](../c/convert_to_scalar.md)
  - [get_variable_numdistinct](../g/get_variable_numdistinct.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - [scalarineqsel](../s/scalarineqsel.md)
  - [prefix_selectivity](../p/prefix_selectivity.md)

## Notes and Other Information
- Returns -1 if no histogram is available or histogram is incompatible
- Uses binary search for O(log n) performance on large histograms
- Attempts to get current min/max values when probing histogram endpoints to handle data changes since last ANALYZE
- Performs linear interpolation using `convert_to_scalar` for precise estimates within histogram bins
- Handles first histogram bin specially since it represents leftmost values differently than other bins
- Applies equality selectivity corrections for inclusive operators (<= and >=)
- Clamps results to avoid extreme estimates: uses CLAMP_PROBABILITY with current endpoints, or custom cutoff (0.01/histogram_resolution) otherwise
- Falls back to brute-force search if histogram sort order is incompatible with query operator
- [Result](../R/Result.md) excludes MCV entries and nulls - caller must combine with those statistics
- Exported function used by multiple selectivity estimation routines

## Simplified Source

```c
double
ineq_histogram_selectivity(PlannerInfo *root,
                           VariableStatData *vardata,
                           Oid opoid, FmgrInfo *opproc, bool isgt, bool iseq,
                           Oid collation,
                           Datum constval, Oid consttype)
{
    double hist_selec = -1.0;
    AttStatsSlot sslot;

    // Get histogram statistics if available and compatible
    if (HeapTupleIsValid(vardata->statsTuple) &&
        statistic_proc_security_check(vardata, opproc->fn_oid) &&
        get_attstatsslot(&sslot, vardata->statsTuple,
                         STATISTIC_KIND_HISTOGRAM, InvalidOid,
                         ATTSTATSSLOT_VALUES))
    {
        // Verify histogram compatibility
        if (sslot.nvalues > 1 &&
            sslot.stacoll == collation &&
            comparison_ops_are_compatible(sslot.staop, opoid))
        {
            // Binary search to find histogram position
            double histfrac;
            int lobound = 0;
            int hibound = sslot.nvalues;
            bool have_current_endpoints = false;

            // Update endpoints for 2-value histograms
            if (sslot.nvalues == 2)
                have_current_endpoints = get_actual_variable_range(root, vardata,
                                                                   sslot.staop, collation,
                                                                   &sslot.values[0], &sslot.values[1]);

            // Binary search loop
            while (lobound < hibound)
            {
                int probe = (lobound + hibound) / 2;
                bool ltcmp;

                // Update endpoint values during search if needed
                if (probe == 0 && sslot.nvalues > 2)
                    get_actual_variable_range(root, vardata, sslot.staop, collation,
                                              &sslot.values[0], NULL);
                else if (probe == sslot.nvalues - 1 && sslot.nvalues > 2)
                    get_actual_variable_range(root, vardata, sslot.staop, collation,
                                              NULL, &sslot.values[probe]);

                // Compare with histogram value
                ltcmp = DatumGetBool(FunctionCall2Coll(opproc, collation,
                                                       sslot.values[probe], constval));
                if (isgt)
                    ltcmp = !ltcmp;

                if (ltcmp)
                    lobound = probe + 1;
                else
                    hibound = probe;
            }

            // Calculate selectivity based on search result
            if (lobound <= 0)
            {
                // Constant below histogram range
                histfrac = 0.0;
            }
            else if (lobound >= sslot.nvalues)
            {
                // Constant above histogram range
                histfrac = 1.0;
            }
            else
            {
                // Interpolate within histogram bin
                int i = lobound;
                double eq_selec = 0;
                double val, high, low, binfrac;

                // Calculate equality selectivity if needed
                if (i == 1 || isgt == iseq)
                {
                    double distinct_count = get_variable_numdistinct(vardata, NULL);
                    AttStatsSlot mcvslot;

                    // Subtract MCV count from distinct values
                    if (get_attstatsslot(&mcvslot, vardata->statsTuple,
                                         STATISTIC_KIND_MCV, InvalidOid,
                                         ATTSTATSSLOT_NUMBERS))
                    {
                        distinct_count -= mcvslot.nnumbers;
                        free_attstatsslot(&mcvslot);
                    }

                    if (distinct_count > 1)
                        eq_selec = 1.0 / distinct_count;
                }

                // Linear interpolation within bin
                if (convert_to_scalar(constval, consttype, collation, &val,
                                      sslot.values[i - 1], sslot.values[i],
                                      vardata->vartype, &low, &high))
                {
                    if (high <= low)
                        binfrac = 0.5;
                    else if (val <= low)
                        binfrac = 0.0;
                    else if (val >= high)
                        binfrac = 1.0;
                    else
                    {
                        binfrac = (val - low) / (high - low);
                        // Handle NaN/Infinity from division
                        if (isnan(binfrac) || binfrac < 0.0 || binfrac > 1.0)
                            binfrac = 0.5;
                    }
                }
                else
                {
                    binfrac = 0.5;  // Fallback if conversion fails
                }

                // Calculate final histogram fraction
                histfrac = (double)(i - 1) + binfrac;
                histfrac /= (double)(sslot.nvalues - 1);

                // Adjust for first bin boundary effects
                if (i == 1)
                    histfrac += eq_selec * (1.0 - binfrac);

                // Adjust for equality in operator
                if (isgt == iseq)
                    histfrac -= eq_selec;
            }

            // Convert to final selectivity (flip for > operators)
            hist_selec = isgt ? (1.0 - histfrac) : histfrac;

            // Clamp extreme values
            if (have_current_endpoints)
                CLAMP_PROBABILITY(hist_selec);
            else
            {
                double cutoff = 0.01 / (double)(sslot.nvalues - 1);
                if (hist_selec < cutoff)
                    hist_selec = cutoff;
                else if (hist_selec > 1.0 - cutoff)
                    hist_selec = 1.0 - cutoff;
            }
        }
        else if (sslot.nvalues > 1)
        {
            // Fallback: brute force search for incompatible histogram
            LOCAL_FCINFO(fcinfo, 2);
            int nmatch = 0;

            InitFunctionCallInfoData(*fcinfo, opproc, 2, collation, NULL, NULL);
            fcinfo->args[1].value = constval;
            fcinfo->args[1].isnull = false;

            for (int i = 0; i < sslot.nvalues; i++)
            {
                fcinfo->args[0].value = sslot.values[i];
                fcinfo->args[0].isnull = false;

                Datum result = FunctionCallInvoke(fcinfo);
                if (!fcinfo->isnull && DatumGetBool(result))
                    nmatch++;
            }

            hist_selec = ((double)nmatch) / ((double)sslot.nvalues);

            // Apply cutoff for brute force results
            double cutoff = 0.01 / (double)(sslot.nvalues - 1);
            if (hist_selec < cutoff)
                hist_selec = cutoff;
            else if (hist_selec > 1.0 - cutoff)
                hist_selec = 1.0 - cutoff;
        }

        free_attstatsslot(&sslot);
    }

    return hist_selec;
}
```