# calc_multirangesel

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:291-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L291-L455)

## Overview
Calculates selectivity estimates for multirange operators using statistics about NULL values, empty multiranges, and histogram data.

## Definition
```c
static double calc_multirangesel(TypeCacheEntry *typcache, VariableStatData *vardata, const MultirangeType *constval, Oid operator)
```

## Detailed Description
This function performs sophisticated selectivity estimation by combining multiple statistical factors:

1. **Statistics Extraction**: Retrieves NULL fraction and empty multirange fraction from pg_statistic
2. **Empty Multirange Handling**: For empty constant multiranges, returns precise selectivity based on the specific operator:
   - Overlap operators: return 0.0 (nothing overlaps with empty)
   - Containment operators: return empty_frac (only empty multiranges match)
   - Contains operators: return 1.0 (everything contains empty)
   - Comparison operators: calculated based on empty fraction

3. **Non-Empty Analysis**: For non-empty constants, delegates to calc_hist_selectivity() for histogram-based analysis
4. **Result Combination**: Merges empty and non-empty selectivity estimates, accounting for NULL values

The function handles the statistical reality that multirange columns often contain a significant fraction of empty ranges and NULL values, which affects selectivity calculations differently for each operator.

## Parameters / Member Variables
- `typcache`: Type cache entry for the multirange type
- `vardata`: Variable statistics data from pg_statistic  
- `constval`: The constant multirange value being compared
- `operator`: OID of the multirange operator

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_statistic
  - [get_attstatsslot](../g/get_attstatsslot.md) (for STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - MultirangeIsEmpty
  - [calc_hist_selectivity](calc_hist_selectivity.md)
  - [default_multirange_selectivity](../d/default_multirange_selectivity.md)
  - CLAMP_PROBABILITY

- Called from (representative examples):
  - [multirangesel](../m/multirangesel.md) (when valid constant multirange is available)

## Notes and Other Information
This function represents the core statistical analysis engine for multirange selectivity. It carefully separates the treatment of empty and non-empty multiranges because they behave very differently under various operators. The function ensures proper handling of edge cases like infinite bounds and maintains statistical accuracy by properly weighting different population segments (NULL, empty, non-empty).

## Simplified Source

```c
static double
calc_multirangesel(TypeCacheEntry *typcache, VariableStatData *vardata,
                   const MultirangeType *constval, Oid operator)
{
    double hist_selec, selec;
    float4 empty_frac, null_frac;

    // Extract statistics from pg_statistic
    if (HeapTupleIsValid(vardata->statsTuple)) {
        Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(vardata->statsTuple);
        null_frac = stats->stanullfrac;

        // Get empty multirange fraction
        AttStatsSlot sslot;
        if (get_attstatsslot(&sslot, vardata->statsTuple,
                           STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                           InvalidOid, ATTSTATSSLOT_NUMBERS)) {
            empty_frac = sslot.numbers[0];
            free_attstatsslot(&sslot);
        } else {
            empty_frac = 0.0;  // No empty fraction available
        }
    } else {
        // No statistics available
        null_frac = empty_frac = 0.0;
    }

    if (MultirangeIsEmpty(constval)) {
        // Handle empty constant multirange based on operator semantics
        switch (operator) {
            // Overlap operators: nothing overlaps with empty
            case OID_MULTIRANGE_OVERLAPS_RANGE_OP:
            case OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP:
            // ... other overlap and positioning operators
            case OID_MULTIRANGE_LESS_OP:
                selec = 0.0;
                break;

            // Containment: only empty multiranges are contained by empty
            case OID_RANGE_MULTIRANGE_CONTAINED_OP:
            case OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP:
            case OID_MULTIRANGE_LESS_EQUAL_OP:
                selec = empty_frac;
                break;

            // Contains: everything contains empty
            case OID_MULTIRANGE_CONTAINS_RANGE_OP:
            case OID_MULTIRANGE_CONTAINS_MULTIRANGE_OP:
            case OID_MULTIRANGE_GREATER_EQUAL_OP:
                selec = 1.0;
                break;

            // Greater than: all non-empty > empty
            case OID_MULTIRANGE_GREATER_OP:
                selec = 1.0 - empty_frac;
                break;

            default:
                selec = 0.0;  // Error case
                break;
        }
    } else {
        // Non-empty constant: use histogram analysis
        hist_selec = calc_hist_selectivity(typcache, vardata, constval, operator);
        if (hist_selec < 0.0)
            hist_selec = default_multirange_selectivity(operator);

        // Combine empty and histogram results
        if (operator == OID_RANGE_MULTIRANGE_CONTAINED_OP ||
            operator == OID_MULTIRANGE_MULTIRANGE_CONTAINED_OP) {
            // Empty is contained by anything non-empty
            selec = (1.0 - empty_frac) * hist_selec + empty_frac;
        } else {
            // Empty Op non-empty usually matches nothing
            selec = (1.0 - empty_frac) * hist_selec;
        }
    }

    // Account for NULL values (all operators are strict)
    selec *= (1.0 - null_frac);

    CLAMP_PROBABILITY(selec);
    return selec;
}
```