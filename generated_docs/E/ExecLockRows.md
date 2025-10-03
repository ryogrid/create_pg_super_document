# ExecLockRows

## Location
[src/backend/executor/nodeLockRows.c:38-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeLockRows.c#L38-L290)

## Overview
ExecLockRows is the main execution function for the LockRows plan node that attempts to lock tuples retrieved from its subplan, handling various locking modes and foreign table scenarios.

## Definition
static TupleTableSlot *ExecLockRows(PlanState *pstate)

## Detailed Description
ExecLockRows implements tuple locking functionality in PostgreSQL execution engine. It retrieves tuples from its outer subplan and attempts to lock them according to the specified row marking requirements. The function handles multiple locking scenarios including:

- Regular table tuple locking with different lock modes (SHARE, KEY SHARE, EXCLUSIVE, NO KEY EXCLUSIVE)
- Foreign table locking through FDW interfaces
- EvalPlanQual (EPQ) processing for concurrent update scenarios
- Lock conflict resolution and retry logic
- Serialization failure handling in isolation levels that require it

The function processes each tuple by iterating through all row marks associated with the LockRows node, attempting to acquire the appropriate lock for each marked relation. If locking succeeds and no concurrent updates require EPQ reprocessing, the locked tuple is returned.

## Parameters / Member Variables
- pstate: Pointer to the PlanState structure, cast to LockRowsState internally

## Dependencies
- Functions called/Symbols referenced:
  - [ExecProcNode](ExecProcNode.md) (to get tuples from outer plan)
  - TupIsNull (to check for null tuples)
  - [EvalPlanQualEnd](EvalPlanQualEnd.md)/Begin/SetSlot/Next (EPQ machinery)
  - [ExecGetJunkAttribute](ExecGetJunkAttribute.md) (to extract ctid and tableoid)
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md) (for foreign table operations)
  - [table_tuple_lock](../t/table_tuple_lock.md) (core tuple locking function)
  - IsolationUsesXactSnapshot (isolation level checking)
- Called from (representative examples):
  - [ExecInitLockRows](ExecInitLockRows.md) (sets this as the ExecProcNode function)

## Notes and Other Information
- The function uses a goto label lnext for retry logic when tuples cannot be locked or fail EPQ checks
- Handles the Halloween problem by ignoring self-modified tuples
- Foreign table locking requires FDW to implement RefetchForeignRow callback
- EPQ (EvalPlanQual) testing is triggered when tuples are updated during locking process
- Different lock modes correspond to SQL row locking clauses (FOR SHARE, FOR UPDATE, etc.)
- Function is located at src/backend/executor/nodeLockRows.c:38-290

## Simplified Source

```c
static TupleTableSlot *
ExecLockRows(PlanState *pstate)
{
    LockRowsState *node = castNode(LockRowsState, pstate);
    TupleTableSlot *slot;
    EState *estate = node->ps.state;
    PlanState *outerPlan = outerPlanState(node);
    bool epq_needed;

    CHECK_FOR_INTERRUPTS();

lnext:
    // Get next tuple from subplan
    slot = ExecProcNode(outerPlan);
    if (TupIsNull(slot)) {
        EvalPlanQualEnd(&node->lr_epqstate);
        return NULL;
    }

    epq_needed = false;

    // Attempt to lock each marked tuple
    foreach(lc, node->lr_arowMarks) {
        ExecAuxRowMark *aerm = (ExecAuxRowMark *) lfirst(lc);
        ExecRowMark *erm = aerm->rowmark;

        // Clear any leftover test tuple
        TupleTableSlot *markSlot = EvalPlanQualSlot(&node->lr_epqstate, erm->relation, erm->rti);
        ExecClearTuple(markSlot);

        // Check if this is an active child relation
        if (erm->rti != erm->prti) {
            Datum tableoid = ExecGetJunkAttribute(slot, aerm->toidAttNo, &isNull);
            if (DatumGetObjectId(tableoid) != erm->relid) {
                erm->ermActive = false;
                continue;
            }
        }
        erm->ermActive = true;

        // Get the tuple's ctid
        Datum ctid_datum = ExecGetJunkAttribute(slot, aerm->ctidAttNo, &isNull);

        // Handle foreign tables differently
        if (erm->relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
            FdwRoutine *fdwroutine = GetFdwRoutineForRelation(erm->relation, false);
            bool updated = false;

            fdwroutine->RefetchForeignRow(estate, erm, ctid_datum, markSlot, &updated);
            if (TupIsNull(markSlot))
                goto lnext;  // Couldn't get lock, skip this row
            if (updated)
                epq_needed = true;
            continue;
        }

        // Lock the tuple
        ItemPointerData tid = *((ItemPointer) DatumGetPointer(ctid_datum));
        LockTupleMode lockmode = determine_lock_mode(erm->markType);

        TM_Result test = table_tuple_lock(erm->relation, &tid, estate->es_snapshot,
                                        markSlot, estate->es_output_cid,
                                        lockmode, erm->waitPolicy, lockflags, &tmfd);

        // Handle locking results
        switch (test) {
            case TM_WouldBlock:
            case TM_SelfModified:
            case TM_Deleted:
                goto lnext;  // Skip this tuple
            case TM_Ok:
                if (tmfd.traversed)
                    epq_needed = true;
                break;
            case TM_Updated:
                // Handle serialization failures and updates
                goto lnext;
        }

        erm->curCtid = tid;
    }

    // Perform EPQ testing if needed
    if (epq_needed) {
        EvalPlanQualBegin(&node->lr_epqstate);
        EvalPlanQualSetSlot(&node->lr_epqstate, slot);
        slot = EvalPlanQualNext(&node->lr_epqstate);
        if (TupIsNull(slot))
            goto lnext;
    }

    return slot;
}
```