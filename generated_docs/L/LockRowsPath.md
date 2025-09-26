# LockRowsPath

## Location
[src/include/nodes/pathnodes.h:2360-2366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L2360-L2366)

## Overview
LockRowsPath represents a query execution path node for acquiring row locks in SELECT FOR UPDATE/SHARE operations, encapsulating the locking semantics and cost calculations for row-level locking in PostgreSQL.

## Definition

```c
typedef struct LockRowsPath
{
	Path		path;
	Path	   *subpath;		/* path representing input source */
	List	   *rowMarks;		/* a list of PlanRowMark's */
	int			epqParam;		/* ID of Param for EvalPlanQual re-eval */
} LockRowsPath;
```
## Detailed Description
LockRowsPath is a specialized path node in PostgreSQL's query planner that represents the operation of acquiring row locks for SELECT FOR UPDATE/SHARE queries. It encapsulates the input data source path, the specific row locking requirements, and parameters needed for EvalPlanQual (EPQ) re-evaluation when concurrent updates occur. The path node includes cost calculations that account for the overhead of row locking operations and potential row refetches during concurrent access scenarios.

## Parameters / Member Variables
- `path`: Base Path structure containing common path information like cost estimates, row count, and pathkeys
- `*subpath`: Pointer to the underlying path that provides the input data to be locked
- `*rowMarks`: List of PlanRowMark structures specifying the locking requirements for different relations
- `epqParam`: Parameter ID used for EvalPlanQual re-evaluation when concurrent tuple modifications are detected
## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base structure)
  - [List](List.md) (for rowMarks)
  - [PlanRowMark](../P/PlanRowMark.md) (referenced in rowMarks list)
- Called from (representative examples):
  - [create_lockrows_path](../c/create_lockrows_path.md) (pathnode.c:3665)
  - [create_lockrows_plan](../c/create_lockrows_plan.md) (createplan.c:2792)
  - [create_plan_recurse](../c/create_plan_recurse.md) (createplan.c:529)

## Notes and Other Information
- The path's pathkeys are set to NIL because locking operations can potentially modify sort key columns, making the result order unpredictable
- Cost calculation includes an additional cpu_tuple_cost per row to account for locking overhead and potential refetches
- The path is marked as not parallel-safe since row locking requires coordination that conflicts with parallel execution
- Used specifically for implementing SELECT FOR UPDATE/SHARE semantics in PostgreSQL's query execution engine