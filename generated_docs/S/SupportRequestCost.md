# SupportRequestCost

## Location
src/include/nodes/supportnodes.h: 131 - 143

## Overview
SupportRequestCost is a structure that allows support functions to provide custom execution cost estimates for their target functions, enabling more accurate query planning by the PostgreSQL optimizer.

## Definition

```c
typedef struct SupportRequestCost
{
	NodeTag		type;

	/* Input fields: */
	struct PlannerInfo *root;	/* Planner's infrastructure (could be NULL) */
	Oid			funcid;			/* function we are inquiring about */
	Node	   *node;			/* parse node invoking function, or NULL */

	/* Output fields: */
	Cost		startup;		/* one-time cost */
	Cost		per_tuple;		/* per-evaluation cost */
} SupportRequestCost;
```
## Detailed Description
The SupportRequestCost structure enables PostgreSQL's query planner to obtain more accurate execution cost estimates from support functions. This mechanism allows custom functions to provide domain-specific knowledge about their execution costs, which can be significantly more accurate than generic estimates.

The cost estimate includes two components:
- A one-time startup cost (e.g., initialization overhead)
- A per-execution cost (cost for each tuple processed)

The estimate should only include the cost of executing the target function itself, not the cost of evaluating its arguments. If a support function can provide an estimate, it stores the values in the cost fields and returns the address of the SupportRequestCost node. If no estimate can be made, it returns NULL, causing the planner to fall back to the function's procost field from the system catalog.

## Parameters / Member Variables
**Input fields:**
- : NodeTag identifying this as a SupportRequestCost structure
- : Pointer to PlannerInfo containing planner's infrastructure; may be NULL in some contexts
- : OID of the function being analyzed for cost estimation
- : Parse node that is invoking the target function; can be FuncExpr, OpExpr, DistinctExpr, NullIfExpr, WindowFunc, or other node types; NULL if function arguments cannot be presumed equivalent to calling node's arguments

**Output fields:**
- : One-time startup cost estimate for query initialization
- : Per-evaluation cost estimate for each execution of the function

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - PlannerInfo
  - Node
  - Cost

- Called from (representative examples):
  - add_function_cost (src/backend/optimizer/util/plancat.c:2102)
  - test_support_func (src/test/regress/regress.c:1060)

## Notes and Other Information
- The node parameter can be NULL when the function cannot assume its arguments are equivalent to what the calling node presents (e.g., for aggregate support functions or per-column comparison operators used by RowExprs)
- Unlike procost (which is automatically scaled by cpu_operator_cost), the Cost request outputs are not automatically scaled - support functions must handle scaling appropriately themselves
- This mechanism is particularly valuable for complex functions with variable execution costs that depend on argument values or other runtime factors
- The cost estimates help the planner make better decisions about query execution strategies, especially in complex queries with multiple execution path options
- Part of PostgreSQL's extensible type system that allows custom functions to integrate deeply with the query optimization process