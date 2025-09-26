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
  - statistic_proc_security_check
  - get_attstatsslot
  - comparison_ops_are_compatible
  - get_actual_variable_range
  - FunctionCall2Coll
  - convert_to_scalar
  - get_variable_numdistinct
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - free_attstatsslot
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - scalarineqsel
  - prefix_selectivity

## Notes and Other Information
- Returns -1 if no histogram is available or histogram is incompatible
- Uses binary search for O(log n) performance on large histograms
- Attempts to get current min/max values when probing histogram endpoints to handle data changes since last ANALYZE
- Performs linear interpolation using `convert_to_scalar` for precise estimates within histogram bins
- Handles first histogram bin specially since it represents leftmost values differently than other bins
- Applies equality selectivity corrections for inclusive operators (<= and >=)
- Clamps results to avoid extreme estimates: uses CLAMP_PROBABILITY with current endpoints, or custom cutoff (0.01/histogram_resolution) otherwise
- Falls back to brute-force search if histogram sort order is incompatible with query operator
- Result excludes MCV entries and nulls - caller must combine with those statistics
- Exported function used by multiple selectivity estimation routines