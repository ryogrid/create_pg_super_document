# SubPlan

## Location
[src/include/nodes/primnodes.h:1059-1095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1059-L1095)

## Overview
SubPlan represents an executable expression node for a subplan (sub-SELECT) that replaces SubLink nodes in expression trees after the planner has finished planning the subquery.

## Definition

```c
typedef struct SubPlan
{
	pg_node_attr(no_query_jumble)

	Expr		xpr;
	/* Fields copied from original SubLink: */
	SubLinkType subLinkType;	/* see above */
	/* The combining operators, transformed to an executable expression: */
	Node	   *testexpr;		/* OpExpr or RowCompareExpr expression tree */
	List	   *paramIds;		/* IDs of Params embedded in the above */
	/* Identification of the Plan tree to use: */
	int			plan_id;		/* Index (from 1) in PlannedStmt.subplans */
	/* Identification of the SubPlan for EXPLAIN and debugging purposes: */
	char	   *plan_name;		/* A name assigned during planning */
	/* Extra data useful for determining subplan's output type: */
	Oid			firstColType;	/* Type of first column of subplan result */
	int32		firstColTypmod; /* Typmod of first column of subplan result */
	Oid			firstColCollation;	/* Collation of first column of subplan
									 * result */
	/* Information about execution strategy: */
	bool		useHashTable;	/* true to store subselect output in a hash
								 * table (implies we are doing "IN") */
	bool		unknownEqFalse; /* true if it's okay to return FALSE when the
								 * spec result is UNKNOWN; this allows much
								 * simpler handling of null values */
	bool		parallel_safe;	/* is the subplan parallel-safe? */
	/* Note: parallel_safe does not consider contents of testexpr or args */
	/* Information for passing params into and out of the subselect: */
	/* setParam and parParam are lists of integers (param IDs) */
	List	   *setParam;		/* initplan and MULTIEXPR subqueries have to
								 * set these Params for parent plan */
	List	   *parParam;		/* indices of input Params from parent plan */
	List	   *args;			/* exprs to pass as parParam values */
	/* Estimated execution costs: */
	Cost		startup_cost;	/* one-time setup cost */
	Cost		per_call_cost;	/* cost for each subplan evaluation */
} SubPlan;
```
## Detailed Description
SubPlan is created by the planner to replace SubLink nodes after subquery planning is complete. It references a sub-plantree stored in the subplans list of the toplevel PlannedStmt, avoiding direct links to make expression tree copying easier without causing multiple subplan processing.

For ordinary subplans, testexpr contains an executable expression (OpExpr, AND/OR tree of OpExprs, or RowCompareExpr) for combining operators. The left-hand arguments are original lefthand expressions, while right-hand arguments are PARAM_EXEC Param nodes representing sub-select outputs.

When a sub-select becomes an initplan rather than a subplan, the executable expression becomes part of the outer plan's expression tree, and testexpr is set to NULL to avoid duplication.

The planner derives lists of values needed for passing into and out of the subplan. Input values are represented as "args" expressions evaluated in outer-query context, assigned to global PARAM_EXEC params indexed by parParam. The setParam list contains PARAM_EXEC params computed by the sub-select for initplan or MULTIEXPR plans.

## Parameters / Member Variables
- `xpr`: Base Expr node structure
- `subLinkType`: Type of sublink copied from original SubLink
- `*testexpr`: OpExpr or RowCompareExpr expression tree for combining operators
- `*paramIds`: IDs of Params embedded in the testexpr
- `plan_id`: Index (from 1) in PlannedStmt.subplans identifying the Plan tree to use
- `*plan_name`: Name assigned during planning for EXPLAIN and debugging purposes
- `firstColType`: Type of first column of subplan result
- `firstColTypmod`: Typmod of first column of subplan result
- `firstColCollation`: Collation of first column of subplan result
- `useHashTable`: True to store subselect output in hash table (implies "IN" operation)
- `unknownEqFalse`: True if okay to return FALSE when spec result is UNKNOWN
- `parallel_safe`: Whether the subplan is parallel-safe
- `*setParam`: List of PARAM_EXEC params set by initplan/MULTIEXPR subqueries for parent plan
- `*parParam`: Indices of input Params from parent plan
- `*args`: Expressions to pass as parParam values
- `startup_cost`: One-time setup cost
- `per_call_cost`: Cost for each subplan evaluation
## Dependencies
- Functions called/Symbols referenced:
  - [SubLinkType](SubLinkType.md)
  - Cost
- Called from (representative examples):
  - [ExecSubPlan](../E/ExecSubPlan.md)
  - [ExecInitSubPlan](../E/ExecInitSubPlan.md)
  - [make_subplan](../m/make_subplan.md)
  - [build_subplan](../b/build_subplan.md)
  - [cost_subplan](../c/cost_subplan.md)

## Notes and Other Information
- The parallel_safe field does not consider contents of testexpr or args
- parParam and setParam are integer Lists (not Bitmapsets) because their ordering is significant
- Costs include the subquery proper cost, testexpr evaluation, and hashtable management overhead
- Runtime coercion functions may be inserted in the executable expression
- PARAM_SUBLINK nodes from original SubLink are replaced by suitably numbered PARAM_EXEC nodes