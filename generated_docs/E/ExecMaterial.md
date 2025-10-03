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
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)
  - [tuplestore_set_eflags](../t/tuplestore_set_eflags.md)
  - [tuplestore_alloc_read_pointer](../t/tuplestore_alloc_read_pointer.md)
  - [tuplestore_ateof](../t/tuplestore_ateof.md)
  - [tuplestore_advance](../t/tuplestore_advance.md)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)
  - [tuplestore_puttupleslot](../t/tuplestore_puttupleslot.md)
  - outerPlanState
  - [ExecProcNode](ExecProcNode.md)
  - TupIsNull
  - [ExecCopySlot](ExecCopySlot.md)
  - [ExecClearTuple](ExecClearTuple.md)
- Called from (representative examples):
  - [ExecInitMaterial](ExecInitMaterial.md)

## Notes and Other Information
- The tuplestore is only initialized when needed (when eflags != 0)
- Supports mark/restore functionality by allocating a second read pointer
- Uses work_mem for tuplestore memory management
- Implements EOF tracking for both the tuplestore and underlying subplan to optimize performance
- The function is robust against multiple calls after returning NULL

## Simplified Source

```c
static TupleTableSlot *
ExecMaterial(PlanState *pstate)
{
    MaterialState *node = castNode(MaterialState, pstate);
    EState *estate = node->ss.ps.state;
    ScanDirection dir = estate->es_direction;
    bool forward = ScanDirectionIsForward(dir);
    Tuplestorestate *tuplestorestate = node->tuplestorestate;
    TupleTableSlot *slot = node->ss.ps.ps_ResultTupleSlot;

    CHECK_FOR_INTERRUPTS();

    // Initialize tuplestore on first call if needed
    if (tuplestorestate == NULL && node->eflags != 0) {
        tuplestorestate = tuplestore_begin_heap(true, false, work_mem);
        tuplestore_set_eflags(tuplestorestate, node->eflags);

        // Allocate mark pointer if needed
        if (node->eflags & EXEC_FLAG_MARK) {
            tuplestore_alloc_read_pointer(tuplestorestate, node->eflags);
        }
        node->tuplestorestate = tuplestorestate;
    }

    // Check if we're at end of tuplestore
    bool eof_tuplestore = (tuplestorestate == NULL) ||
                         tuplestore_ateof(tuplestorestate);

    // Handle backward scan direction at EOF
    if (!forward && eof_tuplestore && !node->eof_underlying) {
        if (!tuplestore_advance(tuplestorestate, forward))
            return NULL;
        eof_tuplestore = false;
    }

    // Try to fetch from tuplestore first
    if (!eof_tuplestore) {
        if (tuplestore_gettupleslot(tuplestorestate, forward, false, slot))
            return slot;
        if (forward)
            eof_tuplestore = true;
    }

    // Fetch new tuple from subplan if needed
    if (eof_tuplestore && !node->eof_underlying) {
        PlanState *outerNode = outerPlanState(node);
        TupleTableSlot *outerslot = ExecProcNode(outerNode);

        if (TupIsNull(outerslot)) {
            node->eof_underlying = true;
            return NULL;
        }

        // Store new tuple in tuplestore
        if (tuplestorestate)
            tuplestore_puttupleslot(tuplestorestate, outerslot);

        ExecCopySlot(slot, outerslot);
        return slot;
    }

    // Nothing left
    return ExecClearTuple(slot);
}
```