# subquery_planner

## Location
[src/backend/optimizer/plan/planner.c:629-1155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L629-L1155)

## Overview
The subquery_planner function is PostgreSQL's primary per-Query planning routine that performs comprehensive query preprocessing, optimization setup, and delegates to grouping_planner for the main planning work.

## Definition
```c
PlannerInfo *subquery_planner(PlannerGlobal *glob, Query *parse, PlannerInfo *parent_root,
                             bool hasRecursion, double tuple_fraction,
                             SetOperationStmt *setops)
```

## Detailed Description
The subquery_planner function serves as the comprehensive query-level planning coordinator, handling all preprocessing tasks that should be performed exactly once per Query object. It creates and initializes a PlannerInfo structure to track planning state, then systematically processes various query components through multiple optimization phases.

Key processing phases include:
1. PlannerInfo initialization with query-level state management
2. WITH clause processing (CTE handling)
3. MERGE command transformation
4. FROM clause normalization and empty jointree replacement
5. SubLink transformation (EXISTS, ANY subqueries to joins)
6. Function RTE preprocessing and inlining
7. Subquery pullup optimization
8. UNION ALL flattening for simple cases
9. Range table entry classification and analysis
10. Permission checking for view access
11. RowMark preprocessing
12. Comprehensive expression preprocessing across all query components
13. HAVING clause optimization (potential movement to WHERE)
14. Outer join reduction to inner joins where possible
15. Useless RTE_RESULT removal and join tree simplification

After preprocessing, it delegates the core planning work to grouping_planner, then handles final cleanup including parameter identification, initPlan cost accounting, and cheapest path selection.

## Parameters / Member Variables
- `glob`: PlannerGlobal structure containing global planning state shared across all query levels
- `parse`: Query structure produced by parser and rewriter containing the SQL statement to plan
- `parent_root`: PlannerInfo of the immediate parent query (NULL for top-level queries)
- `hasRecursion`: Boolean flag indicating if this is a recursive WITH query requiring special parameter handling
- `tuple_fraction`: Expected fraction of result tuples to be retrieved (affects optimization decisions)
- `setops`: SetOperationStmt context for set operation subqueries to guide path generation (NULL for non-set operations)

## Dependencies
- Functions called/Symbols referenced:
  - [SS_process_ctes](../S/SS_process_ctes.md) (WITH clause processing)
  - [transform_MERGE_to_join](../t/transform_MERGE_to_join.md) (MERGE transformation)
  - [pull_up_sublinks](../p/pull_up_sublinks.md) (SubLink optimization)
  - [pull_up_subqueries](../p/pull_up_subqueries.md) (subquery pullup)
  - [preprocess_expression](../p/preprocess_expression.md) (expression preprocessing)
  - [preprocess_qual_conditions](../p/preprocess_qual_conditions.md) (WHERE/JOIN condition processing)
  - [grouping_planner](../g/grouping_planner.md) (main planning logic)
  - [reduce_outer_joins](../r/reduce_outer_joins.md) (outer join optimization)
  - [SS_identify_outer_params](../S/SS_identify_outer_params.md) (parameter identification)
  - [has_subclass](../h/has_subclass.md) (inheritance checking)
- Called from (representative examples):
  - [standard_planner](standard_planner.md) (top-level planning)
  - [set_subquery_pathlist](set_subquery_pathlist.md) (subquery planning)
  - [make_subplan](../m/make_subplan.md) (subplan creation)
  - [SS_process_ctes](../S/SS_process_ctes.md) (CTE processing)
  - [recurse_set_operations](../r/recurse_set_operations.md) (set operation handling)

## Notes and Other Information
- Returns PlannerInfo containing all planning results, with final paths in UPPERREL_FINAL upperrel
- Performs extensive query tree analysis to optimize subsequent planning phases
- Handles complex permission checking for views to prevent information leakage
- Implements sophisticated HAVING clause optimization with group-aware movement to WHERE
- Manages join alias variable cleanup after preprocessing to prevent scan hazards
- Supports both regular and recursive query planning with appropriate parameter handling
- Coordinates with global planner state for cross-query-level optimizations
- Located in src/backend/optimizer/plan/planner.c:629-1155

## Simplified Source

```c
PlannerInfo *subquery_planner(PlannerGlobal *glob, Query *parse, PlannerInfo *parent_root,
                             bool hasRecursion, double tuple_fraction,
                             SetOperationStmt *setops) {
    // Create and initialize PlannerInfo structure
    PlannerInfo *root = makeNode(PlannerInfo);
    root->parse = parse;
    root->glob = glob;
    root->query_level = parent_root ? parent_root->query_level + 1 : 1;
    root->parent_root = parent_root;

    // Initialize all planner state fields
    root->plan_params = NIL;
    root->init_plans = NIL;
    root->eq_classes = NIL;
    root->hasRecursion = hasRecursion;

    // Set up recursion parameter if needed
    if (hasRecursion)
        root->wt_param_id = assign_special_exec_param(root);

    // Create top-level join domain
    root->join_domains = list_make1(makeNode(JoinDomain));

    // Process WITH clauses (CTEs)
    if (parse->cteList)
        SS_process_ctes(root);

    // Transform MERGE commands
    transform_MERGE_to_join(parse);

    // Replace empty FROM clause with dummy RTE
    replace_empty_jointree(parse);

    // Transform SubLinks to joins
    if (parse->hasSubLinks)
        pull_up_sublinks(root);

    // Process function RTEs
    preprocess_function_rtes(root);

    // Pull up subqueries where possible
    pull_up_subqueries(root);

    // Flatten simple UNION ALL
    if (parse->setOperations)
        flatten_simple_union_all(root);

    // Analyze range table entries
    bool hasOuterJoins = false;
    bool hasResultRTEs = false;
    foreach(l, parse->rtable) {
        RangeTblEntry *rte = lfirst_node(RangeTblEntry, l);

        switch (rte->rtekind) {
            case RTE_JOIN:
                root->hasJoinRTEs = true;
                if (IS_OUTER_JOIN(rte->jointype))
                    hasOuterJoins = true;
                break;
            case RTE_RESULT:
                hasResultRTEs = true;
                break;
        }

        if (rte->lateral)
            root->hasLateralRTEs = true;
    }

    // Check view permissions
    foreach(l, parse->rtable) {
        RangeTblEntry *rte = lfirst_node(RangeTblEntry, l);
        if (rte->perminfoindex != 0 && rte->relkind == RELKIND_VIEW) {
            RTEPermissionInfo *perminfo = getRTEPermissionInfo(parse->rteperminfos, rte);
            if (!ExecCheckOneRelPerms(perminfo))
                aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_VIEW, get_rel_name(perminfo->relid));
        }
    }

    // Preprocess expressions in all query parts
    parse->targetList = (List *) preprocess_expression(root, (Node *) parse->targetList, EXPRKIND_TARGET);
    parse->havingQual = preprocess_expression(root, parse->havingQual, EXPRKIND_QUAL);
    preprocess_qual_conditions(root, (Node *) parse->jointree);

    // Process other expression types (LIMIT, window clauses, etc.)
    parse->limitOffset = preprocess_expression(root, parse->limitOffset, EXPRKIND_LIMIT);
    parse->limitCount = preprocess_expression(root, parse->limitCount, EXPRKIND_LIMIT);

    // Optimize HAVING clause - move to WHERE if possible
    List *newHaving = NIL;
    foreach(l, (List *) parse->havingQual) {
        Node *havingclause = (Node *) lfirst(l);

        if (contain_agg_clause(havingclause) || contain_volatile_functions(havingclause)) {
            // Keep in HAVING
            newHaving = lappend(newHaving, havingclause);
        } else if (parse->groupClause && !parse->groupingSets) {
            // Move to WHERE
            parse->jointree->quals = (Node *) lappend((List *) parse->jointree->quals, havingclause);
        } else {
            // Copy to both WHERE and HAVING
            parse->jointree->quals = (Node *) lappend((List *) parse->jointree->quals, copyObject(havingclause));
            newHaving = lappend(newHaving, havingclause);
        }
    }
    parse->havingQual = (Node *) newHaving;

    // Reduce outer joins to inner joins where possible
    if (hasOuterJoins)
        reduce_outer_joins(root);

    // Remove useless result RTEs
    if (hasResultRTEs || hasOuterJoins)
        remove_useless_result_rtes(root);

    // Do the main planning work
    grouping_planner(root, tuple_fraction, setops);

    // Final cleanup: identify parameters, charge for initplans, set cheapest path
    SS_identify_outer_params(root);
    RelOptInfo *final_rel = fetch_upper_rel(root, UPPERREL_FINAL, NULL);
    SS_charge_for_initplans(root, final_rel);
    set_cheapest(final_rel);

    return root;
}
```