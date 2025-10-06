# calc_hist_selectivity

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:452-699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L452-L699)

## Overview
Calculates multirange operator selectivity using histograms of multirange bounds for non-empty, non-NULL values.

## Definition
```c
static double calc_hist_selectivity(TypeCacheEntry *typcache, VariableStatData *vardata, const MultirangeType *constval, Oid operator)
```

## Detailed Description
This function performs histogram-based selectivity estimation for multirange operators. It handles the complex logic for different operator types by analyzing bound relationships and using statistical histograms of range bounds and lengths.

The function processes different operator categories:
1. **Comparison operators** (<, <=, >, >=): Compare based on lower bounds
2. **Positioning operators** (<<, >>): Compare bounds across ranges
3. **Overlap operators** (&&, &<, &>): Use complement probability calculation
4. **Containment operators** (@>, <@): Use specialized containment analysis with length histograms

## Parameters / Member Variables
- `typcache`: Type cache entry for the multirange type
- `vardata`: Variable statistics data from pg_statistic
- `constval`: The constant multirange value being compared
- `operator`: OID of the multirange operator

## Dependencies
- Functions called/Symbols referenced:
  - statistic_proc_security_check
  - get_attstatsslot
  - range_deserialize
  - multirange_get_bounds
  - calc_hist_selectivity_scalar
  - calc_hist_selectivity_contains
  - calc_hist_selectivity_contained
  - free_attstatsslot

- Called from (representative examples):
  - calc_multirangesel (for non-empty constant multiranges)

## Notes and Other Information
This function represents the core histogram analysis for multirange selectivity estimation. It carefully handles security checks for statistics functions and manages complex operator semantics. The function requires both bounds histograms and length histograms for containment operators.

## Simplified Source

```c
static double
calc_hist_selectivity(TypeCacheEntry *typcache, VariableStatData *vardata,
                     const MultirangeType *constval, Oid operator)
{
    TypeCacheEntry *rng_typcache = typcache->rngtype;
    AttStatsSlot hslot, lslot;
    int nhist;
    RangeBound *hist_lower, *hist_upper;
    RangeBound const_lower, const_upper, tmp;
    double hist_selec;
    int i;

    // Security checks for statistics functions
    if (!statistic_proc_security_check(vardata, rng_typcache->rng_cmp_proc_finfo.fn_oid))
        return -1;
    if (OidIsValid(rng_typcache->rng_subdiff_finfo.fn_oid) &&
        !statistic_proc_security_check(vardata, rng_typcache->rng_subdiff_finfo.fn_oid))
        return -1;

    // Get bounds histogram
    if (!(HeapTupleIsValid(vardata->statsTuple) &&
          get_attstatsslot(&hslot, vardata->statsTuple,
                          STATISTIC_KIND_BOUNDS_HISTOGRAM, InvalidOid,
                          ATTSTATSSLOT_VALUES)))
        return -1.0;

    if (hslot.nvalues < 2) {
        free_attstatsslot(&hslot);
        return -1.0;
    }

    // Convert range histogram to bound histograms
    nhist = hslot.nvalues;
    hist_lower = (RangeBound *) palloc(sizeof(RangeBound) * nhist);
    hist_upper = (RangeBound *) palloc(sizeof(RangeBound) * nhist);
    for (i = 0; i < nhist; i++) {
        bool empty;
        range_deserialize(rng_typcache, DatumGetRangeTypeP(hslot.values[i]),
                         &hist_lower[i], &hist_upper[i], &empty);
        // Error handling for empty ranges in histogram
    }

    // Get length histogram for containment operators
    if (operator == OID_MULTIRANGE_CONTAINS_RANGE_OP ||
        operator == OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP ||
        operator == OID_MULTIRANGE_RANGE_CONTAINED_OP ||
        operator == OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP) {
        if (!(HeapTupleIsValid(vardata->statsTuple) &&
              get_attstatsslot(&lslot, vardata->statsTuple,
                              STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                              InvalidOid, ATTSTATSSLOT_VALUES))) {
            free_attstatsslot(&hslot);
            return -1.0;
        }
    } else {
        memset(&lslot, 0, sizeof(lslot));
    }

    // Extract constant bounds (first lower, last upper)
    multirange_get_bounds(rng_typcache, constval, 0, &const_lower, &tmp);
    multirange_get_bounds(rng_typcache, constval, constval->rangeCount - 1,
                         &tmp, &const_upper);

    // Calculate selectivity based on operator type
    switch (operator) {
        case OID_MULTIRANGE_LESS_OP:
            hist_selec = calc_hist_selectivity_scalar(rng_typcache, &const_lower,
                                                     hist_lower, nhist, false);
            break;

        case OID_MULTIRANGE_LESS_EQUAL_OP:
            hist_selec = calc_hist_selectivity_scalar(rng_typcache, &const_lower,
                                                     hist_lower, nhist, true);
            break;

        case OID_MULTIRANGE_GREATER_OP:
            hist_selec = 1 - calc_hist_selectivity_scalar(rng_typcache, &const_lower,
                                                         hist_lower, nhist, false);
            break;

        case OID_MULTIRANGE_GREATER_EQUAL_OP:
            hist_selec = 1 - calc_hist_selectivity_scalar(rng_typcache, &const_lower,
                                                         hist_lower, nhist, true);
            break;

        case OID_MULTIRANGE_LEFT_RANGE_OP:
        case OID_MULTIRANGE_LEFT_MULTIRANGE_OP:
            // var << const when upper(var) < lower(const)
            hist_selec = calc_hist_selectivity_scalar(rng_typcache, &const_lower,
                                                     hist_upper, nhist, false);
            break;

        case OID_MULTIRANGE_RIGHT_RANGE_OP:
        case OID_MULTIRANGE_RIGHT_MULTIRANGE_OP:
            // var >> const when lower(var) > upper(const)
            hist_selec = 1 - calc_hist_selectivity_scalar(rng_typcache, &const_upper,
                                                         hist_lower, nhist, true);
            break;

        case OID_MULTIRANGE_OVERLAPS_RANGE_OP:
        case OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP:
        case OID_MULTIRANGE_CONTAINS_ELEM_OP:
            // A && B <=> NOT (A << B OR A >> B)
            hist_selec = calc_hist_selectivity_scalar(rng_typcache, &const_lower,
                                                     hist_upper, nhist, false);
            hist_selec += (1.0 - calc_hist_selectivity_scalar(rng_typcache, &const_upper,
                                                             hist_lower, nhist, true));
            hist_selec = 1.0 - hist_selec;
            break;

        case OID_MULTIRANGE_CONTAINS_RANGE_OP:
        case OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP:
            hist_selec = calc_hist_selectivity_contains(rng_typcache, &const_lower,
                                                       &const_upper, hist_lower, nhist,
                                                       lslot.values, lslot.nvalues);
            break;

        case OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP:
        case OID_RANGE_MULTIRANGE_CONTAINED_OP:
            if (const_lower.infinite) {
                hist_selec = calc_hist_selectivity_scalar(rng_typcache, &const_upper,
                                                         hist_upper, nhist, true);
            } else if (const_upper.infinite) {
                hist_selec = 1.0 - calc_hist_selectivity_scalar(rng_typcache, &const_lower,
                                                               hist_lower, nhist, false);
            } else {
                hist_selec = calc_hist_selectivity_contained(rng_typcache, &const_lower,
                                                           &const_upper, hist_lower, nhist,
                                                           lslot.values, lslot.nvalues);
            }
            break;

        default:
            hist_selec = -1.0;  // Unknown operator
            break;
    }

    free_attstatsslot(&lslot);
    free_attstatsslot(&hslot);
    return hist_selec;
}
```