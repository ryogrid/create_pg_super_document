# SupportRequestRows

## Location
[src/include/nodes/supportnodes.h:158-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/supportnodes.h#L158-L169)

## Overview
SupportRequestRows is a structure that enables support functions to provide custom output rowcount estimates for set-returning functions, helping PostgreSQL's query planner make more accurate decisions about query execution strategies.

## Definition

```c
typedef struct SupportRequestRows
{
	NodeTag		type;

	/* Input fields: */
	struct PlannerInfo *root;	/* Planner's infrastructure (could be NULL) */
	Oid			funcid;			/* function we are inquiring about */
	Node	   *node;			/* parse node invoking function */

	/* Output fields: */
	double		rows;			/* number of rows expected to be returned */
} SupportRequestRows;
```
## Detailed Description
The SupportRequestRows structure allows PostgreSQL's query planner to obtain custom rowcount estimates from support functions for set-returning functions (SRFs). This mechanism is crucial for accurate query planning when dealing with functions that return multiple rows, as the number of rows returned can significantly impact the choice of execution strategies.

Set-returning functions can have highly variable output row counts depending on their input parameters and internal logic. By providing custom row estimates, support functions can give the planner much more accurate information than the generic prorows value stored in the system catalog.

When a support function can provide an estimate, it stores the expected row count in the rows field and returns the address of the SupportRequestRows node. If no estimate can be made, it returns NULL, causing the planner to fall back to the target function's prorows field.

## Parameters / Member Variables
**Input fields:**
- : NodeTag identifying this as a SupportRequestRows structure
- : Pointer to PlannerInfo containing planner's infrastructure; may be NULL in some contexts
- : OID of the set-returning function being analyzed for row count estimation
- : Parse node that is invoking the target function; currently always a FuncExpr or OpExpr

**Output fields:**
- : Estimated number of rows expected to be returned by the function execution

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - [PlannerInfo](../P/PlannerInfo.md)
  - [Node](../N/Node.md)

- Called from (representative examples):
  - [get_function_rows](../g/get_function_rows.md) (src/backend/optimizer/util/plancat.c:2165)
  - [array_unnest_support](../a/array_unnest_support.md) (src/backend/utils/adt/arrayfuncs.c:6338)
  - [generate_series_int4_support](../g/generate_series_int4_support.md) (src/backend/utils/adt/int.c:1590)
  - [generate_series_int8_support](../g/generate_series_int8_support.md) (src/backend/utils/adt/int8.c:1464)
  - [test_support_func](../t/test_support_func.md) (src/test/regress/regress.c:1070)

## Notes and Other Information
- This mechanism applies specifically to set-returning functions (functions that can return multiple rows)
- The node parameter is currently always a FuncExpr or OpExpr when invoking set-returning functions
- Accurate row count estimation is particularly important for set-returning functions because they can dramatically affect join ordering, memory usage, and overall query execution strategy
- Examples of functions that benefit from this include generate_series(), unnest(), and other functions whose output size depends on input parameters
- The row estimate directly influences the planner's cost calculations and join ordering decisions
- This is part of PostgreSQL's extensible optimization framework that allows custom functions to provide detailed optimization hints to the query planner
- Support functions should provide realistic estimates based on the actual input parameters when possible, as overly optimistic or pessimistic estimates can lead to poor query performance