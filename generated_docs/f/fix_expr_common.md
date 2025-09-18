# fix_expr_common

## Location
src/backend/optimizer/plan/setrefs.c: 1978 - 2072

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
  - set_opfuncid (resolves operator function OIDs)
  - set_sa_opfuncid (resolves scalar array operator function OIDs)
  - lappend_oid (adds OIDs to relation list)
  - [DatumGetObjectId](../D/DatumGetObjectId.md) (extracts OID from Datum)
  - lappend_int (adds integers to list)
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