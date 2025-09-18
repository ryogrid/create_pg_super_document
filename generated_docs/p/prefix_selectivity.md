# prefix_selectivity

## Location
[src/backend/utils/adt/like_support.c:1232-1315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L1232-L1315)

## Overview
Estimates the selectivity of a fixed prefix pattern by calculating the fraction of rows that would match the prefix using range-based histogram analysis.

## Definition
```c
static Selectivity prefix_selectivity(PlannerInfo *root, VariableStatData *vardata,
                                      Oid eqopr, Oid ltopr, Oid geopr,
                                      Oid collation,
                                      Const *prefixcon)
```

## Detailed Description
This function estimates the selectivity of a fixed prefix pattern by modeling it as a range query. For a prefix "foo", it estimates the selectivity as the fraction of values that satisfy "variable >= 'foo' AND variable < 'fop'", where "fop" is the next possible string after "foo".

The algorithm works in several steps:
1. **Lower bound estimation**: Uses `ineq_histogram_selectivity` to estimate "x >= prefix"
2. **Upper bound creation**: Calls `make_greater_string` to create the next possible string after the prefix
3. **Upper bound estimation**: If successful, estimates "x < upper_bound" selectivity
4. **Range combination**: Merges the two selectivities using range query logic
5. **Clamping**: Ensures the result is at least as large as the equality selectivity to avoid unrealistically small estimates

The function handles edge cases where histograms are unavailable (returns DEFAULT_MATCH_SEL) and where prefixes are so long that histogram bins cannot distinguish the boundaries effectively.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and configuration
- `vardata`: VariableStatData containing statistics for the column being analyzed
- `eqopr`: OID of the equality operator for the column data type
- `ltopr`: OID of the less-than operator for range upper bound estimation
- `geopr`: OID of the greater-than-or-equal operator for range lower bound estimation
- `collation`: OID of the collation to use for string comparisons
- `prefixcon`: Const node containing the fixed prefix string to analyze

## Dependencies
- Functions called/Symbols referenced:
  - [get_opcode](../g/get_opcode.md) (to get operator implementation function)
  - [fmgr_info](../f/fmgr_info.md) (to initialize function manager info)
  - ineq_histogram_selectivity (core histogram-based selectivity estimation)
  - [make_greater_string](../m/make_greater_string.md) (creates next possible string for upper bound)
  - [var_eq_const](../v/var_eq_const.md) (estimates equality selectivity for clamping)
  - DEFAULT_MATCH_SEL (fallback selectivity when no histogram available)
- Called from:
  - [patternsel_common](patternsel_common.md) (main pattern selectivity estimation function)

## Notes and Other Information
- This is a static function within like_support.c, not exposed in the public API
- The function assumes histogram statistics are available and falls back to defaults when they are not
- Uses range query selectivity merging logic: `topsel + prefixsel - 1.0`
- The clamping mechanism prevents unrealistically small estimates for long prefixes where histogram resolution is insufficient
- Works with any collation and locale, though accuracy may vary
- The comment notes that upper-bound estimation is used even when locale reliability is questionable, since approximate correctness is sufficient for query planning