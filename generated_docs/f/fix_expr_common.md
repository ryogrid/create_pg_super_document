# fix_expr_common

## Location
[src/backend/optimizer/plan/setrefs.c:1978-2072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L1978-L2072)

## Overview
A core utility function that performs generic expression node processing during plan reference fixing, handling operator function lookups, dependency tracking, and expression metadata updates.

## Definition
```c
static void fix_expr_common(PlannerInfo *root, Node *node)
```

## Detailed Description
The `fix_expr_common` function serves as the central processing point for expression nodes during the set_plan_references phase of query planning. It handles several critical tasks common to all expression-fixing variants:

1. **Operator Function Lookup**: Resolves operator function OIDs for OpExpr, DistinctExpr, NullIfExpr, and ScalarArrayOpExpr nodes by calling set_opfuncid() and related functions.

2. **Dependency Tracking**: Records function dependencies in root->glob->invalItems for user-defined functions (Aggref, WindowFunc, FuncExpr, and operator functions) to ensure proper plan invalidation when functions are dropped or modified.

3. **Relation OID Collection**: Adds OIDs from regclass Const nodes to root->glob->relationOids for dependency tracking.

4. **Grouping Expression Processing**: Fills in column index lists for GROUPING() expressions using the root->grouping_map.

The function assumes it's safe to update opcode information in-place and may modify the planner's input data structures.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning context and dependency tracking lists
- `node`: Expression node to be processed for reference fixing

## Dependencies
- Functions called/Symbols referenced:
  - [record_plan_function_dependency](../r/record_plan_function_dependency.md) (tracks function dependencies)
  - [set_opfuncid](../s/set_opfuncid.md) (resolves operator function OIDs)
  - [set_sa_opfuncid](../s/set_sa_opfuncid.md) (resolves scalar array operator function OIDs)
  - [lappend_oid](../l/lappend_oid.md) (adds OIDs to relation list)
  - [DatumGetObjectId](../D/DatumGetObjectId.md) (extracts OID from Datum)
  - [lappend_int](../l/lappend_int.md) (adds integers to list)
  - lfirst_int (extracts integer from list cell)
  - [equal](../e/equal.md) (compares expression lists)
  - ISREGCLASSCONST (macro to check regclass constants)
- Called from (representative examples):
  - [fix_scan_expr_mutator](fix_scan_expr_mutator.md)
  - [fix_scan_expr_walker](fix_scan_expr_walker.md)
  - [fix_join_expr_mutator](fix_join_expr_mutator.md)
  - [fix_upper_expr_mutator](fix_upper_expr_mutator.md)
  - [extract_query_dependencies_walker](../e/extract_query_dependencies_walker.md)

## Notes and Other Information
- Handles multiple expression node types: Aggref, WindowFunc, FuncExpr, OpExpr, DistinctExpr, NullIfExpr, ScalarArrayOpExpr, Const, GroupingFunc
- Critical for plan invalidation system - ensures plans are invalidated when referenced functions or relations are modified
- Updates expression nodes in-place, which is acceptable during the set_plan_references phase
- Part of PostgreSQL's plan finalization process that converts planner data structures into executable plans
- The grouping_map processing is specifically for handling GROUPING() expressions in queries with grouping sets

## Simplified Source

```c
static void
fix_expr_common(PlannerInfo *root, Node *node)
{
    // Handle aggregate function nodes
    if (IsA(node, Aggref))
    {
        record_plan_function_dependency(root,
                                       ((Aggref *) node)->aggfnoid);
    }
    // Handle window function nodes
    else if (IsA(node, WindowFunc))
    {
        record_plan_function_dependency(root,
                                       ((WindowFunc *) node)->winfnoid);
    }
    // Handle regular function calls
    else if (IsA(node, FuncExpr))
    {
        record_plan_function_dependency(root,
                                       ((FuncExpr *) node)->funcid);
    }
    // Handle operator expressions
    else if (IsA(node, OpExpr))
    {
        set_opfuncid((OpExpr *) node);
        record_plan_function_dependency(root,
                                       ((OpExpr *) node)->opfuncid);
    }
    // Handle DISTINCT expressions
    else if (IsA(node, DistinctExpr))
    {
        set_opfuncid((OpExpr *) node);  // Same structure as OpExpr
        record_plan_function_dependency(root,
                                       ((DistinctExpr *) node)->opfuncid);
    }
    // Handle NULLIF expressions
    else if (IsA(node, NullIfExpr))
    {
        set_opfuncid((OpExpr *) node);  // Same structure as OpExpr
        record_plan_function_dependency(root,
                                       ((NullIfExpr *) node)->opfuncid);
    }
    // Handle scalar array operator expressions (ANY/ALL)
    else if (IsA(node, ScalarArrayOpExpr))
    {
        ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) node;

        set_sa_opfuncid(saop);
        record_plan_function_dependency(root, saop->opfuncid);

        // Record additional function dependencies if present
        if (OidIsValid(saop->hashfuncid))
            record_plan_function_dependency(root, saop->hashfuncid);

        if (OidIsValid(saop->negfuncid))
            record_plan_function_dependency(root, saop->negfuncid);
    }
    // Handle constant values
    else if (IsA(node, Const))
    {
        Const *con = (Const *) node;

        // Track regclass references for dependency tracking
        if (ISREGCLASSCONST(con))
            root->glob->relationOids =
                lappend_oid(root->glob->relationOids,
                           DatumGetObjectId(con->constvalue));
    }
    // Handle GROUPING() function expressions
    else if (IsA(node, GroupingFunc))
    {
        GroupingFunc *g = (GroupingFunc *) node;
        AttrNumber *grouping_map = root->grouping_map;

        Assert(grouping_map || g->cols == NIL);

        if (grouping_map)
        {
            ListCell *lc;
            List *cols = NIL;

            // Map grouping references to column indices
            foreach(lc, g->refs)
            {
                cols = lappend_int(cols, grouping_map[lfirst_int(lc)]);
            }

            Assert(!g->cols || equal(cols, g->cols));

            if (!g->cols)
                g->cols = cols;
        }
    }
}
```