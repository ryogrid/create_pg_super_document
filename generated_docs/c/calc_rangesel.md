# calc_rangesel

## Location
[src/backend/utils/adt/rangetypes_selfuncs.c:230-364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_selfuncs.c#L230-L364)

## Overview
Calculates selectivity estimates for range operators using column statistics, considering NULL fractions, empty range fractions, and histogram data.

## Definition
```c
static double calc_rangesel(TypeCacheEntry *typcache, VariableStatData *vardata, const RangeType *constval, Oid operator)
```

## Detailed Description
This function computes selectivity estimates for range operators by analyzing column statistics from pg_statistic. It considers three main factors:
1. NULL fraction from column statistics
2. Empty range fraction from range-specific statistics
3. Histogram-based selectivity for non-empty, non-null values

The function handles empty range constants as special cases, since they have well-defined behavior with different operators (e.g., everything contains an empty range, nothing overlaps with an empty range). For non-empty constants, it delegates to histogram analysis and then combines the results with empty range statistics.

## Parameters / Member Variables
- `*typcache`: TypeCacheEntry containing range type information
- `*vardata`: VariableStatData structure with column statistics
- `*constval`: The constant range value to compare against
- `operator`: OID of the range operator being evaluated

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleIsValid
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - free_attstatsslot
  - [RangeIsEmpty](../R/RangeIsEmpty.md)
  - [calc_hist_selectivity](../c/calc_hist_selectivity.md)
  - [default_range_selectivity](../d/default_range_selectivity.md)
  - CLAMP_PROBABILITY
  - Various OID_RANGE_* operator constants

## Notes and Other Information
- This is a static function used internally by rangesel
- Handles the complex logic of combining different statistical measures
- Provides fallback estimates when statistics are unavailable
- All range operators are strict (return NULL for NULL inputs)
- Uses pg_statistic data for accurate selectivity estimation

## Simplified Source

```c
static double calc_rangesel(TypeCacheEntry *typcache, VariableStatData *vardata,
                           const RangeType *constval, Oid operator) {
    double hist_selec;
    double selec;
    float4 empty_frac, null_frac;

    // Extract NULL and empty range fractions from statistics
    if (HeapTupleIsValid(vardata->statsTuple)) {
        Form_pg_statistic stats;
        AttStatsSlot sslot;

        stats = (Form_pg_statistic) GETSTRUCT(vardata->statsTuple);
        null_frac = stats->stanullfrac;

        // Get empty range fraction from statistics
        if (get_attstatsslot(&sslot, vardata->statsTuple,
                           STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                           InvalidOid, ATTSTATSSLOT_NUMBERS)) {
            empty_frac = sslot.numbers[0];
            free_attstatsslot(&sslot);
        } else {
            empty_frac = 0.0;
        }
    } else {
        // No statistics available
        null_frac = 0.0;
        empty_frac = 0.0;
    }

    if (RangeIsEmpty(constval)) {
        // Handle empty range constants with operator-specific logic
        switch (operator) {
            case OID_RANGE_OVERLAP_OP:
            case OID_RANGE_OVERLAPS_LEFT_OP:
            case OID_RANGE_OVERLAPS_RIGHT_OP:
            case OID_RANGE_LEFT_OP:
            case OID_RANGE_RIGHT_OP:
            case OID_RANGE_LESS_OP:
                selec = 0.0;  // Nothing matches these with empty range
                break;

            case OID_RANGE_CONTAINED_OP:
            case OID_RANGE_LESS_EQUAL_OP:
                selec = empty_frac;  // Only empty ranges match
                break;

            case OID_RANGE_CONTAINS_OP:
            case OID_RANGE_GREATER_EQUAL_OP:
                selec = 1.0;  // Everything matches
                break;

            case OID_RANGE_GREATER_OP:
                selec = 1.0 - empty_frac;  // All non-empty ranges match
                break;

            default:
                selec = 0.0;
                break;
        }
    } else {
        // Use histogram analysis for non-empty ranges
        hist_selec = calc_hist_selectivity(typcache, vardata, constval, operator);
        if (hist_selec < 0.0)
            hist_selec = default_range_selectivity(operator);

        // Combine histogram results with empty range statistics
        if (operator == OID_RANGE_CONTAINED_OP) {
            selec = (1.0 - empty_frac) * hist_selec + empty_frac;
        } else {
            selec = (1.0 - empty_frac) * hist_selec;
        }
    }

    // Account for NULL values (all range operators are strict)
    selec *= (1.0 - null_frac);

    CLAMP_PROBABILITY(selec);
    return selec;
}
```