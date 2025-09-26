# generic_restriction_selectivity

## Location
src/backend/utils/adt/selfuncs.c: 915 - 1041

## Overview
General-purpose selectivity estimation function for operators without specialized knowledge, using standard MCV and histogram statistics to provide reasonable selectivity estimates.

## Definition
```c
double
generic_restriction_selectivity(PlannerInfo *root, Oid oproid, Oid collation,
                                List *args, int varRelid,
                                double default_selectivity)
```

## Detailed Description
`generic_restriction_selectivity` serves as the fallback selectivity estimation function for operators that lack specialized knowledge but work on data types with standard statistics. The function implements a comprehensive approach:

1. **Pattern Recognition**: Identifies if the expression matches "VAR OP CONST" or "CONST OP VAR" patterns
2. **MCV Analysis**: Uses `mcv_selectivity` to get exact selectivity for most common values
3. **Histogram Estimation**: Applies `histogram_selectivity` for non-MCV population
4. **Adaptive Weighting**: For small histograms (10-100 entries), blends histogram and default estimates
5. **Null Handling**: Accounts for null fraction in final calculation
6. **Range Clamping**: Ensures results stay within reasonable probability bounds

The function assumes the operator is strict and immutable/stable, and treats the histogram as a reasonable random sample of non-MCV values.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context
- `oproid`: OID of the operator for which selectivity is being estimated  
- `collation`: Collation OID for string comparison operations
- `args`: List of arguments to the operator (should be variable and constant)
- `varRelid`: Relation ID if expression involves a specific relation (0 for general case)
- `default_selectivity`: Fallback selectivity estimate to use when statistics are insufficient

## Dependencies
- Functions called/Symbols referenced:
  - get_restriction_variable
  - mcv_selectivity
  - histogram_selectivity
  - get_opcode
  - fmgr_info
  - ReleaseVariableStats
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - matchingsel
  - Various operator selectivity functions as generic fallback

## Notes and Other Information
- Returns default_selectivity if expression is not "VAR OP CONST" pattern
- Returns 0.0 for comparisons with NULL constants (assumes strict operators)
- Uses histogram with min_hist_size=10 and n_skip=1 parameters
- For histograms smaller than 100 entries, applies weighted combination with default estimate
- Clamps final results to [0.0001, 0.9999] range to avoid extreme estimates
- Combines MCV and histogram using formula: selec = mcvsel + (1 - nullfrac - mcvsum) * hist_selec
- Best suited for operators whose semantics are not strongly related to histogram ordering
- Requires standard PostgreSQL statistics collection (MCV and histogram) to be effective