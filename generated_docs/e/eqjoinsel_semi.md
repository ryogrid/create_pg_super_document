# eqjoinsel_semi

## Location
src/backend/utils/adt/selfuncs.c: 2635 - 2822

## Overview
Computes join selectivity for semi and anti joins, implementing specialized logic that differs significantly from inner joins by estimating the fraction of outer relation rows that have at least one matching partner in the inner relation.

## Definition


## Detailed Description
This function estimates selectivity for semi and anti joins, which fundamentally ask "which outer relation rows have at least one match in the inner relation?" This differs from inner joins that count all matching pairs.

**Key distinguishing features:**
1. **Asymmetric clamping**: Only nd2 (inner relation distinct values) is clamped to the inner relation size, not nd1. This prevents double-counting selectivity of outer relation restrictions.
2. **Match-based approach**: When MCVs are available, it calculates exact matches for the MCV portion, then estimates the uncertain remainder.
3. **Heuristic estimation**: For non-MCV populations, it uses the heuristic that if nd1 ≤ nd2, most outer rows likely have matches; otherwise, the fraction nd2/nd1 have matches.

**Estimation strategies:**
- **With MCVs**: Computes exact matches for MCV portions, then applies heuristics to estimate uncertain rows
- **Without MCVs**: Falls back to pure heuristic based on distinct value counts
- **Clamping logic**: Ensures nd2 doesn't exceed inner relation size, preventing impossible estimates

The function handles the case where opfuncoid might be InvalidOid (unlike eqjoinsel_inner), making it more robust for complex query scenarios.

## Parameters / Member Variables
- : OID of the equality comparison function (may be InvalidOid)
- : Collation to use for string comparisons
- : Variable statistical data for LHS (outer relation)
- : Variable statistical data for RHS (inner relation)
- , : Number of distinct values for each variable
- , : Whether the distinct value counts are defaults
- , : Attribute statistics slots containing MCV data
- , : PostgreSQL statistics forms with null fractions
- , : Whether MCV statistics are available for each side
- : RelOptInfo for the inner relation (used for size clamping)

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - CLAMP_PROBABILITY
  - LOCAL_FCINFO
- Called from (representative examples):
  - [eqjoinsel](eqjoinsel.md)

## Notes and Other Information
- Used for both SEMI and ANTI joins since they're estimated the same way
- The asymmetric treatment of nd1 vs nd2 is crucial for correct selectivity estimation in the broader query planning context
- When reliable ndistinct values aren't available, the function conservatively assumes 50% of uncertain rows have join partners
- The clamping mechanism accounts for cases where the inner relation is very small or empty
- Unlike inner join estimation, this focuses on existence of matches rather than counting all match combinations
- The function carefully manages memory allocation for temporary match tracking arrays
- Results represent the fraction of outer relation rows that will survive the semi/anti join condition