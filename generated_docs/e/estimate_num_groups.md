# estimate_num_groups

## Location
[src/backend/utils/adt/selfuncs.c:3429-3810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L3429-L3810)

## Overview
Estimates the number of distinct groups that will result from a GROUP BY clause or DISTINCT operation, accounting for correlation between variables and using statistical data to provide accurate cardinality estimates for query planning.

## Definition

```c
double
estimate_num_groups(PlannerInfo *root, List *groupExprs, double input_rows,
					List **pgset, EstimationInfo *estinfo)
```
## Detailed Description
This function is central to PostgreSQL's GROUP BY and DISTINCT cardinality estimation. It analyzes grouping expressions to predict how many distinct groups will be produced, which is essential for cost estimation of grouping operations, hash tables, and sort operations.

The algorithm uses a sophisticated multi-step approach:

1. **Boolean Expression Handling**: Boolean expressions contribute exactly 2 groups regardless of complexity
2. **Variable Extraction**: Complex expressions are reduced to their component variables, treating f(x) similarly to x since functions rarely increase distinct values
3. **Equivalence Class Processing**: Variables from different relations known to be equal are deduplicated, keeping the one with the best statistics
4. **Per-Relation Processing**: For variables within each relation, it multiplies distinct value estimates, applies clamping heuristics, and adjusts for restriction selectivity
5. **Cross-Relation Combination**: Results from different relations are multiplied together
6. **Set-Returning Function Adjustment**: Accounts for functions that return multiple rows per input

The function includes advanced features like multivariate statistics support and handles edge cases such as volatile functions (which could produce unique results for each input row).

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and statistics
- : List of expressions in the GROUP BY clause or DISTINCT operation
- : Estimated number of rows feeding into the grouping operation
- : Optional pointer to grouping set filter (for GROUPING SETS functionality)
- : Optional output parameter to return estimation metadata and flags

## Dependencies
- Functions called/Symbols referenced:
  - [clamp_row_est](../c/clamp_row_est.md): Ensures row estimates stay within reasonable bounds
  - examine_variable: Extracts statistics for variables and expressions
  - [add_unique_group_var](../a/add_unique_group_var.md): Maintains deduplicated list of grouping variables
  - [expression_returns_set_rows](expression_returns_set_rows.md): Handles set-returning functions in GROUP BY
  - [pull_var_clause](../p/pull_var_clause.md): Extracts variables from complex expressions
  - [contain_volatile_functions](../c/contain_volatile_functions.md): Detects expressions with unpredictable results
  - estimate_multivariate_ndistinct: Uses extended statistics for correlated variables
  - EstimationInfo/SELFLAG_USED_DEFAULT: Tracks when default estimates are used
- Called from (representative examples):
  - [get_number_of_groups](../g/get_number_of_groups.md): Primary interface for GROUP BY cardinality estimation
  - [create_unique_path](../c/create_unique_path.md): Used for DISTINCT operation planning
  - [cost_incremental_sort](../c/cost_incremental_sort.md): Helps estimate costs for incremental sorting with grouping

## Notes and Other Information
- Never returns zero groups to avoid division-by-zero in downstream calculations
- Applies a "fudge factor" (dividing by 10) when multiple variables from the same relation are present, acknowledging likely correlation
- Uses advanced mathematical formulas for adjusting estimates based on restriction selectivity, accounting for sampling without replacement
- Supports PostgreSQL's extended statistics (multivariate n-distinct) when available
- Handles GROUPING SETS by filtering expressions through the pgset parameter
- Applies clamping to prevent estimates from exceeding the number of input rows or falling below 1
- The algorithm assumes that join clauses and relations not containing grouped variables don't affect group count
- Includes special handling for expressional indexes when statistics are available for the entire expression