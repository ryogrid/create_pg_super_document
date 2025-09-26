# LockRows

## Location
[src/include/nodes/plannodes.h:1256-1261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L1256-L1261)

## Overview
LockRows is a plan node structure that represents row-locking operations in PostgreSQL's execution plan tree, implementing FOR UPDATE/SHARE locking semantics during query execution.

## Definition

```c
typedef struct LockRows
{
	Plan		plan;
	List	   *rowMarks;		/* a list of PlanRowMark's */
	int			epqParam;		/* ID of Param for EvalPlanQual re-eval */
} LockRows;
```
## Detailed Description
The LockRows node is responsible for acquiring row-level locks on tuples as they pass through the execution pipeline. It serves as a plan node that wraps around other plan nodes to add locking functionality. The node is particularly important in implementing SQL's FOR UPDATE and FOR SHARE clauses, which require explicit row-level locking for concurrency control.

The LockRows node works by processing tuples from its child plan and applying the appropriate locks based on the PlanRowMark specifications. It integrates with PostgreSQL's EvalPlanQual (EPQ) mechanism to handle concurrent updates by providing a parameter that forces re-evaluation of the plan when concurrent modifications are detected.

## Parameters / Member Variables
- : Base Plan structure containing common plan node information like cost estimates, target lists, and child plan references
- : List of PlanRowMark structures that specify which relations should be locked and how they should be locked (FOR UPDATE, FOR SHARE, etc.)
- : Parameter ID used by EvalPlanQual for re-evaluation of the plan when concurrent tuple modifications are detected during execution

## Dependencies
- Functions called/Symbols referenced:
  - [Plan](../P/Plan.md) (base structure)
  - [List](List.md) (PostgreSQL list structure)
  - [PlanRowMark](../P/PlanRowMark.md) (locking specification structure)
- Called from (representative examples):
  - [ExecInitLockRows](../E/ExecInitLockRows.md) (executor initialization)
  - [create_lockrows_plan](../c/create_lockrows_plan.md) (planner)
  - [ExecInitNode](../E/ExecInitNode.md) (generic executor initialization)
  - [make_lockrows](../m/make_lockrows.md) (plan creation utility)

## Notes and Other Information
- The rowMarks list should be a subset of the rowMarks listed in the top-level PlannedStmt to maintain consistency
- All scan nodes below this LockRows node must depend on the epqParam to ensure proper re-evaluation during EvalPlanQual processing
- The LockRows node is typically created during the planning phase when FOR UPDATE/SHARE clauses are present in the query
- Integration with PostgreSQL's MVCC system through the EvalPlanQual mechanism ensures proper handling of concurrent modifications
- The node supports various locking strengths and policies as defined by the associated PlanRowMark structures