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
  - [examine_variable](examine_variable.md): Extracts statistics for variables and expressions
  - [add_unique_group_var](../a/add_unique_group_var.md): Maintains deduplicated list of grouping variables
  - [expression_returns_set_rows](expression_returns_set_rows.md): Handles set-returning functions in GROUP BY
  - [pull_var_clause](../p/pull_var_clause.md): Extracts variables from complex expressions
  - [contain_volatile_functions](../c/contain_volatile_functions.md): Detects expressions with unpredictable results
  - [estimate_multivariate_ndistinct](estimate_multivariate_ndistinct.md): Uses extended statistics for correlated variables
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

## Simplified Source

```c
double
estimate_num_groups(PlannerInfo *root, List *groupExprs, double input_rows,
                    List **pgset, EstimationInfo *estinfo)
{
    List *varinfos = NIL;
    double srf_multiplier = 1.0;
    double numdistinct = 1.0;

    // Initialize output parameter and ensure input_rows >= 1
    if (estinfo != NULL)
        memset(estinfo, 0, sizeof(EstimationInfo));
    input_rows = clamp_row_est(input_rows);

    // Special case: no grouping columns means exactly one group
    if (groupExprs == NIL || (pgset && *pgset == NIL))
        return 1.0;

    // Process each grouping expression
    foreach(lc, groupExprs) {
        Node *groupexpr = lfirst(lc);

        // Skip if not in current grouping set
        if (pgset && !list_member_int(*pgset, i++))
            continue;

        // Track set-returning functions
        double this_srf_multiplier = expression_returns_set_rows(root, groupexpr);
        if (srf_multiplier < this_srf_multiplier)
            srf_multiplier = this_srf_multiplier;

        // Boolean expressions contribute exactly 2 groups
        if (exprType(groupexpr) == BOOLOID) {
            numdistinct *= 2.0;
            continue;
        }

        // Try to get statistics for the entire expression
        examine_variable(root, groupexpr, 0, &vardata);
        if (HeapTupleIsValid(vardata.statsTuple) || vardata.isunique) {
            varinfos = add_unique_group_var(root, varinfos, groupexpr, &vardata);
            ReleaseVariableStats(vardata);
            continue;
        }
        ReleaseVariableStats(vardata);

        // Extract component variables from expression
        List *varshere = pull_var_clause(groupexpr, PVC_RECURSE_FLAGS);

        // Handle volatile functions (each row could be unique)
        if (varshere == NIL) {
            if (contain_volatile_functions(groupexpr))
                return input_rows;
            continue;
        }

        // Add each variable to processing list
        foreach(lc2, varshere) {
            Node *var = lfirst(lc2);
            examine_variable(root, var, 0, &vardata);
            varinfos = add_unique_group_var(root, varinfos, var, &vardata);
            ReleaseVariableStats(vardata);
        }
    }

    // Handle case with only constants/booleans
    if (varinfos == NIL) {
        numdistinct *= srf_multiplier;
        return clamp_row_est(numdistinct);
    }

    // Process variables grouped by relation
    while (varinfos != NIL) {
        GroupVarInfo *varinfo1 = linitial(varinfos);
        RelOptInfo *rel = varinfo1->rel;
        double reldistinct = 1.0;
        List *relvarinfos = NIL;
        List *newvarinfos = NIL;

        // Separate variables for current relation vs others
        foreach(lc, varinfos) {
            GroupVarInfo *varinfo = lfirst(lc);
            if (varinfo->rel == rel)
                relvarinfos = lappend(relvarinfos, varinfo);
            else
                newvarinfos = lappend(newvarinfos, varinfo);
        }

        // Calculate distinct values for this relation
        // Try multivariate statistics first, then individual estimates
        while (relvarinfos) {
            double mvndistinct;
            if (estimate_multivariate_ndistinct(root, rel, &relvarinfos, &mvndistinct)) {
                reldistinct *= mvndistinct;
            } else {
                // Use individual variable estimates
                foreach(lc, relvarinfos) {
                    GroupVarInfo *varinfo = lfirst(lc);
                    reldistinct *= varinfo->ndistinct;
                    if (estinfo && varinfo->isdefault)
                        estinfo->flags |= SELFLAG_USED_DEFAULT;
                }
                relvarinfos = NIL;
            }
        }

        // Apply clamping and selectivity adjustments
        if (rel->tuples > 0) {
            double clamp = rel->tuples;

            // Apply correlation fudge factor for multiple variables
            if (relvarcount > 1)
                clamp *= 0.1;

            if (reldistinct > clamp)
                reldistinct = clamp;

            // Adjust for restriction selectivity using sampling formula
            if (reldistinct > 0 && rel->rows < rel->tuples) {
                reldistinct *= (1 - pow((rel->tuples - rel->rows) / rel->tuples,
                                      rel->tuples / reldistinct));
            }

            numdistinct *= clamp_row_est(reldistinct);
        }

        varinfos = newvarinfos;
    }

    // Apply SRF multiplier and final bounds checking
    numdistinct *= srf_multiplier;
    numdistinct = ceil(numdistinct);

    if (numdistinct > input_rows)
        numdistinct = input_rows;
    if (numdistinct < 1.0)
        numdistinct = 1.0;

    return numdistinct;
}
```