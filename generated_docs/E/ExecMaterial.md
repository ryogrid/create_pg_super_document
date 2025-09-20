# ExecMaterial

## Location
[src/backend/executor/nodeMaterial.c:39-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMaterial.c#L39-L163)

## Overview
ExecMaterial is the main execution function for Material nodes that collects and buffers tuples from its subplan in a tuplestore, allowing for backward scanning, rescanning, and mark/restore operations.

## Definition

```c
structure
	 */
	matstate = makeNode(MaterialState);
```
## Detailed Description
ExecMaterial implements a buffering strategy for tuple access. It operates in a lazy manner - as long as it's at the end of collected data, it fetches one new row from the subplan on each call and stores it in the tuplestore before returning it. The tuplestore is only read when backward scanning, rescanning, or mark/restore operations are requested.

The function handles both forward and backward scan directions. For forward scans, it tries to read from the tuplestore first, and if at EOF, fetches new tuples from the subplan. For backward scans, it reads from the tuplestore. The function maintains state to track whether the underlying subplan has reached EOF to avoid unnecessary calls.

## Parameters / Member Variables
- : The PlanState containing the MaterialState node and execution context

## Dependencies
- Functions called/Symbols referenced:
  - [MaterialState](../M/MaterialState.md)
  - ScanDirection
  - ScanDirectionIsForward
  - tuplestore_begin_heap
  - tuplestore_set_eflags
  - tuplestore_alloc_read_pointer
  - [tuplestore_ateof](../t/tuplestore_ateof.md)
  - [tuplestore_advance](../t/tuplestore_advance.md)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)
  - tuplestore_puttupleslot
  - outerPlanState
  - ExecProcNode
  - TupIsNull
  - ExecCopySlot
  - ExecClearTuple
- Called from (representative examples):
  - [ExecInitMaterial](ExecInitMaterial.md)

## Notes and Other Information
- The tuplestore is only initialized when needed (when eflags != 0)
- Supports mark/restore functionality by allocating a second read pointer
- Uses work_mem for tuplestore memory management
- Implements EOF tracking for both the tuplestore and underlying subplan to optimize performance
- The function is robust against multiple calls after returning NULL