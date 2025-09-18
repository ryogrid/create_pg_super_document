# calc_multirangesel

## Location
src/backend/utils/adt/multirangetypes_selfuncs.c: 291 - 455

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
  - get_attstatsslot (for STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM)
  - free_attstatsslot
  - MultirangeIsEmpty
  - calc_hist_selectivity
  - default_multirange_selectivity
  - CLAMP_PROBABILITY

- Called from (representative examples):
  - multirangesel (when valid constant multirange is available)

## Notes and Other Information
This function represents the core statistical analysis engine for multirange selectivity. It carefully separates the treatment of empty and non-empty multiranges because they behave very differently under various operators. The function ensures proper handling of edge cases like infinite bounds and maintains statistical accuracy by properly weighting different population segments (NULL, empty, non-empty).