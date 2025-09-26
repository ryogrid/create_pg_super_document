# SubqueryNext

## Location
[src/backend/executor/nodeSubqueryscan.c:46-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubqueryscan.c#L46-L66)

## Overview
SubqueryNext is a static function that retrieves the next tuple from a subquery execution, serving as a workhorse function for ExecSubqueryScan.

## Definition
```c
static TupleTableSlot *SubqueryNext(SubqueryScanState *node)
```

## Detailed Description
SubqueryNext is a core function in PostgreSQL's subquery scanning mechanism. It acts as an intermediary between the subquery scan executor and the underlying subplan execution. The function directly returns the result slot from the subplan execution without performing expensive tuple copying operations, optimizing performance by reusing the existing tuple slot. This design choice reflects PostgreSQL's emphasis on minimizing unnecessary data movement during query execution.

## Parameters / Member Variables
- `node`: A SubqueryScanState pointer containing the execution state for the subquery scan, including the subplan to be executed

## Dependencies
- Functions called/Symbols referenced:
  - [ExecProcNode](../E/ExecProcNode.md) (executes the next step of the subplan)
  - [SubqueryScanState](SubqueryScanState.md) (execution state structure)
- Called from (representative examples):
  - [ExecSubqueryScan](../E/ExecSubqueryScan.md) (main subquery scan execution function)

## Notes and Other Information
- The function is marked as static, indicating it's only used within the nodeSubqueryscan.c file
- Performance optimization: Returns the subplan's result slot directly rather than copying tuples with ExecCopySlot()
- The node's own ScanTupleSlot is reserved specifically for EvalPlanQual rechecks, not for normal tuple flow
- Located at src/backend/executor/nodeSubqueryscan.c:46-66