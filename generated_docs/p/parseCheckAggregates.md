# parseCheckAggregates

## Location
[src/backend/parser/parse_agg.c:1078-1274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1078-L1274)

## Overview
Validates aggregate function placement and grouping correctness after query parsing is complete, checking for misplaced aggregates and improper grouping violations.

## Definition

```c
structs.
		 */
		List	   *gsets = expand_grouping_sets(qry->groupingSets, qry->groupDistinct, 4096);
```
## Detailed Description
This function performs comprehensive validation of aggregate functions and grouping in SQL queries:

1. **Grouping sets processing**: If grouping sets are present, expands them (with a 4096 limit to prevent resource issues) and finds the intersection of all sets to determine common grouping columns
2. **Range table analysis**: Scans the range table to identify JOIN entries and self-referencing CTEs, which affect subsequent processing
3. **GROUP BY clause processing**: Builds a list of acceptable grouping expressions from the GROUP BY clause, flattening join alias variables when necessary for correct equality comparisons
4. **Variable classification**: Separates simple Vars from complex expressions in grouping clauses and identifies variables common to all grouping sets for functional dependency checking
5. **Target list validation**: Checks both regular and resjunk target list elements for ungrouped variables, including those from ORDER BY and WINDOW clauses
6. **HAVING clause validation**: Applies the same ungrouped variable checks to the HAVING clause
7. **GROUPING expression finalization**: Processes GROUPING() expressions within both target lists and HAVING clauses
8. **Recursive query validation**: Enforces the SQL standard restriction that aggregate functions cannot appear in recursive terms

The function handles complex grouping scenarios including grouping sets, functional dependencies, and join alias flattening.

## Parameters / Member Variables
- : Parser state containing context information and flags like p_hasAggs
- : Query structure containing target list, GROUP BY clause, HAVING clause, and grouping sets

## Dependencies
- Functions called/Symbols referenced:
  - [expand_grouping_sets](../e/expand_grouping_sets.md)
  - [list_intersection_int](../l/list_intersection_int.md)  
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md)
  - [flatten_join_alias_vars](../f/flatten_join_alias_vars.md)
  - [finalize_grouping_exprs](../f/finalize_grouping_exprs.md)
  - [check_ungrouped_columns](../c/check_ungrouped_columns.md)
  - [locate_agg_of_level](../l/locate_agg_of_level.md)
- Called from (representative examples):
  - [transformSelectStmt](../t/transformSelectStmt.md)
  - [transformDeleteStmt](../t/transformDeleteStmt.md)
  - [transformSetOperationStmt](../t/transformSetOperationStmt.md)

## Notes and Other Information
- Should only be called when aggregates, GROUP BY, HAVING, or grouping sets are present
- Most misplaced aggregates are caught earlier in transformAggregateCall, but this provides additional validation
- The 4096 grouping set limit is arbitrary but prevents pathological resource consumption
- [Join](../J/Join.md) alias flattening is expensive but necessary for correct variable equality determination
- Handles both simple grouping and complex grouping sets scenarios with appropriate optimizations

## Simplified Source

```c
void parseCheckAggregates(ParseState *pstate, Query *qry) {
    List *gset_common = NIL;
    List *groupClauses = NIL;
    List *groupClauseCommonVars = NIL;
    bool have_non_var_grouping;
    List *func_grouped_rels = NIL;
    bool hasJoinRTEs = false;
    bool hasSelfRefRTEs = false;

    // Should only be called when aggregates/grouping are present
    Assert(pstate->p_hasAggs || qry->groupClause ||
           qry->havingQual || qry->groupingSets);

    // Handle grouping sets if present
    if (qry->groupingSets) {
        // Expand grouping sets (max 4096 to prevent resource issues)
        List *gsets = expand_grouping_sets(qry->groupingSets, qry->groupDistinct, 4096);

        if (!gsets)
            ereport(ERROR, "too many grouping sets present (maximum 4096)");

        // Find intersection of all grouping sets
        gset_common = linitial(gsets);
        for (int i = 1; i < list_length(gsets) && gset_common; i++) {
            gset_common = list_intersection_int(gset_common, list_nth(gsets, i));
        }

        // Optimize single grouping set case
        if (list_length(gsets) == 1 && qry->groupClause)
            qry->groupingSets = NIL;
    }

    // Scan range table for JOIN and self-referencing CTEs
    foreach(lc, pstate->p_rtable) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
        if (rte->rtekind == RTE_JOIN)
            hasJoinRTEs = true;
        else if (rte->rtekind == RTE_CTE && rte->self_reference)
            hasSelfRefRTEs = true;
    }

    // Build list of acceptable GROUP BY expressions
    foreach(lc, qry->groupClause) {
        SortGroupClause *grpcl = (SortGroupClause *) lfirst(lc);
        TargetEntry *expr = get_sortgroupclause_tle(grpcl, qry->targetList);
        if (expr)
            groupClauses = lappend(groupClauses, expr);
    }

    // Flatten join aliases if needed
    if (hasJoinRTEs)
        groupClauses = (List *) flatten_join_alias_vars(NULL, qry,
                                                        (Node *) groupClauses);

    // Classify grouping expressions and find common variables
    have_non_var_grouping = false;
    foreach(lc, groupClauses) {
        TargetEntry *tle = lfirst(lc);
        if (!IsA(tle->expr, Var)) {
            have_non_var_grouping = true;
        } else if (!qry->groupingSets ||
                   list_member_int(gset_common, tle->ressortgroupref)) {
            groupClauseCommonVars = lappend(groupClauseCommonVars, tle->expr);
        }
    }

    // Check target list for ungrouped variables
    Node *clause = (Node *) qry->targetList;
    finalize_grouping_exprs(clause, pstate, qry, groupClauses,
                           hasJoinRTEs, have_non_var_grouping);
    if (hasJoinRTEs)
        clause = flatten_join_alias_vars(NULL, qry, clause);
    check_ungrouped_columns(clause, pstate, qry, groupClauses,
                           groupClauseCommonVars, have_non_var_grouping,
                           &func_grouped_rels);

    // Check HAVING clause for ungrouped variables
    clause = (Node *) qry->havingQual;
    finalize_grouping_exprs(clause, pstate, qry, groupClauses,
                           hasJoinRTEs, have_non_var_grouping);
    if (hasJoinRTEs)
        clause = flatten_join_alias_vars(NULL, qry, clause);
    check_ungrouped_columns(clause, pstate, qry, groupClauses,
                           groupClauseCommonVars, have_non_var_grouping,
                           &func_grouped_rels);

    // Forbid aggregates in recursive queries
    if (pstate->p_hasAggs && hasSelfRefRTEs)
        ereport(ERROR, "aggregate functions not allowed in recursive term");
}
```

**Key Points:**
- Validates aggregate placement and grouping correctness after parsing
- Handles complex grouping sets with intersection logic (max 4096 sets)
- Scans for JOINs and self-referencing CTEs that affect validation
- Builds grouping clause list and flattens join aliases when needed
- Classifies simple vs complex grouping expressions for optimization
- Validates both target list and HAVING clause for ungrouped variables
- Enforces SQL standard: no aggregates in recursive query terms