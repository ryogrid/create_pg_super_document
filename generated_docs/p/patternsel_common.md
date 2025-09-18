# patternsel_common

## Location
[src/backend/utils/adt/like_support.c:486-759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L486-L759)

## Overview
A comprehensive selectivity estimation function for pattern matching operations that combines histogram analysis, most-common-values statistics, and heuristic methods to predict how many rows will match a given pattern.

## Definition
```c
static double patternsel_common(PlannerInfo *root, Oid oprid, Oid opfuncid, List *args, int varRelid, Oid collation, Pattern_Type ptype, bool negate)
```

## Detailed Description
The `patternsel_common` function is PostgreSQL's sophisticated selectivity estimator for pattern matching operations like LIKE, ILIKE, and regular expressions. It provides the query planner with accurate estimates of how many rows will match a pattern, which is crucial for choosing optimal query execution plans.

The function employs a multi-layered approach:

1. **Pattern Analysis**: Extracts fixed prefixes from patterns using `pattern_fixed_prefix` to identify portions that can be estimated more accurately
2. **Exact Match Handling**: For patterns that specify exact values, uses standard equality selectivity estimation via `var_eq_const`
3. **Statistical Analysis**: For non-exact patterns, combines multiple estimation methods:
   - **Histogram Method**: Applies the pattern to histogram entries when sufficient data exists (≥100 entries)
   - **Heuristic Method**: Uses prefix selectivity combined with remainder pattern selectivity for smaller datasets
   - **Hybrid Approach**: Blends histogram and heuristic methods for medium-sized datasets (10-100 entries)
4. **MCV Integration**: Separately analyzes most-common-values by directly applying the pattern operator
5. **Result Composition**: Combines histogram, heuristic, and MCV results while accounting for null values

This sophisticated approach enables PostgreSQL to make informed decisions about index usage, join order, and other execution strategies.

## Parameters / Member Variables
- `root`: Planner information context containing query and table statistics
- `oprid`: OID of the comparison operator (for positive match even when negated)
- `opfuncid`: OID of the underlying function (can be computed from `oprid` if needed)
- `args`: List of arguments to the pattern operation
- `varRelid`: Relation ID for variable statistics lookup
- `collation`: Collation OID to use for pattern matching
- `ptype`: Type of pattern operation (`Pattern_Type` enum: LIKE, ILIKE, regex, etc.)
- `negate`: Whether to estimate selectivity for the negated operation (NOT LIKE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - `get_restriction_variable`: Extracts variable and constant from operation arguments
  - [pattern_fixed_prefix](pattern_fixed_prefix.md): Extracts fixed prefix and estimates remainder selectivity
  - [var_eq_const](../v/var_eq_const.md): Estimates selectivity for exact equality comparisons
  - `histogram_selectivity`: Applies pattern to histogram entries for selectivity estimation
  - [prefix_selectivity](prefix_selectivity.md): Estimates selectivity for prefix-based range operations
  - `mcv_selectivity`: Analyzes most-common-values against the pattern
  - `ReleaseVariableStats`: Frees variable statistics resources
  - `CLAMP_PROBABILITY`: Ensures result stays within valid probability range
- Called from (representative examples):
  - [like_regex_support](../l/like_regex_support.md): Pattern support dispatcher for selectivity requests
  - [patternsel](patternsel.md): Main entry point for pattern selectivity estimation

## Notes and Other Information
- Supports TEXT, NAME, BPCHAR (char), and BYTEA data types
- Returns default estimate (`DEFAULT_MATCH_SEL`) when unable to analyze the pattern or statistics
- Handles null constants by returning 0.0 selectivity (strict operators never match nulls)
- Uses actual operator collation for pattern analysis to ensure runtime cache compatibility
- Automatically coerces text constants to bpchar when needed for type compatibility
- Applies confidence bounds (0.0001 to 0.9999) to prevent extreme selectivity estimates
- Properly accounts for null fraction when computing final selectivity
- For negated operations, computes `1.0 - positive_selectivity - null_fraction`
- Memory management includes cleanup of dynamically allocated prefix constants