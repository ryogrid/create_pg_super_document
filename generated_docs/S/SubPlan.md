# SubPlan

## Location
src/include/nodes/primnodes.h: 1059 - 1095

## Overview
SubPlan represents an executable expression node for a subplan (sub-SELECT) that replaces SubLink nodes in expression trees after the planner has finished planning the subquery.

## Definition


## Detailed Description
SubPlan is created by the planner to replace SubLink nodes after subquery planning is complete. It references a sub-plantree stored in the subplans list of the toplevel PlannedStmt, avoiding direct links to make expression tree copying easier without causing multiple subplan processing.

For ordinary subplans, testexpr contains an executable expression (OpExpr, AND/OR tree of OpExprs, or RowCompareExpr) for combining operators. The left-hand arguments are original lefthand expressions, while right-hand arguments are PARAM_EXEC Param nodes representing sub-select outputs.

When a sub-select becomes an initplan rather than a subplan, the executable expression becomes part of the outer plan's expression tree, and testexpr is set to NULL to avoid duplication.

The planner derives lists of values needed for passing into and out of the subplan. Input values are represented as "args" expressions evaluated in outer-query context, assigned to global PARAM_EXEC params indexed by parParam. The setParam list contains PARAM_EXEC params computed by the sub-select for initplan or MULTIEXPR plans.

## Parameters / Member Variables
- : Base Expr node structure
- : Type of sublink copied from original SubLink
- : OpExpr or RowCompareExpr expression tree for combining operators
- : IDs of Params embedded in the testexpr
- : Index (from 1) in PlannedStmt.subplans identifying the Plan tree to use
- : Name assigned during planning for EXPLAIN and debugging purposes
- : Type of first column of subplan result
- : Typmod of first column of subplan result
- : Collation of first column of subplan result
- : True to store subselect output in hash table (implies "IN" operation)
- : True if okay to return FALSE when spec result is UNKNOWN
- : Whether the subplan is parallel-safe
- : List of PARAM_EXEC params set by initplan/MULTIEXPR subqueries for parent plan
- : Indices of input Params from parent plan
- : Expressions to pass as parParam values
- : One-time setup cost
- : Cost for each subplan evaluation

## Dependencies
- Functions called/Symbols referenced:
  - SubLinkType
  - Cost
- Called from (representative examples):
  - ExecSubPlan
  - ExecInitSubPlan
  - make_subplan
  - build_subplan
  - cost_subplan

## Notes and Other Information
- The parallel_safe field does not consider contents of testexpr or args
- parParam and setParam are integer Lists (not Bitmapsets) because their ordering is significant
- Costs include the subquery proper cost, testexpr evaluation, and hashtable management overhead
- Runtime coercion functions may be inserted in the executable expression
- PARAM_SUBLINK nodes from original SubLink are replaced by suitably numbered PARAM_EXEC nodes