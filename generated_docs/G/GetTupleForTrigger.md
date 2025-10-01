# GetTupleForTrigger

## Location
[src/backend/commands/trigger.c:3371-3508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3371-L3508)

## Overview
Fetches a tuple into a slot for trigger processing, handling tuple locking and Eval Plan Qual (EPQ) operations as necessary to ensure consistent access in concurrent environments.

## Definition
```c
static bool GetTupleForTrigger(EState *estate,
                              EPQState *epqstate,
                              ResultRelInfo *relinfo,
                              ItemPointer tid,
                              LockTupleMode lockmode,
                              TupleTableSlot *oldslot,
                              bool do_epq_recheck,
                              TupleTableSlot **epqslot,
                              TM_Result *tmresultp,
                              TM_FailureData *tmfdp)
```

## Detailed Description
This function is a critical component of PostgreSQL's trigger system that safely retrieves tuples for trigger execution in concurrent environments. It handles two main scenarios: complex locking with EPQ (when epqslot is provided) and simple tuple fetching (when epqslot is NULL).

When EPQ is enabled, the function performs tuple locking using the specified lock mode and handles various concurrency scenarios including self-modification, concurrent updates, and deletions. It uses the Eval Plan Qual mechanism to ensure that triggers operate on consistent data even when tuples are modified concurrently.

The function implements comprehensive error handling for different tuple states and isolation levels, ensuring that triggers are executed with appropriate consistency guarantees.

## Parameters / Member Variables
- `estate`: Execution state containing snapshot and transaction context information
- `epqstate`: EPQ state for handling concurrent tuple modifications (NULL if EPQ not needed)
- `relinfo`: Result relation information containing relation descriptor and metadata
- `tid`: Item pointer (tuple identifier) specifying which tuple to fetch
- `lockmode`: Lock mode to acquire on the tuple (e.g., FOR UPDATE, FOR SHARE)
- `oldslot`: Tuple table slot to store the fetched tuple
- `do_epq_recheck`: Whether to perform EPQ recheck when tuple has been modified
- `epqslot`: Output parameter for EPQ-processed tuple slot (NULL if EPQ not used)
- `tmresultp`: Output parameter for tuple manager operation result
- `tmfdp`: Output parameter for tuple manager failure data

## Dependencies
- Functions called/Symbols referenced:
  - [table_tuple_lock](../t/table_tuple_lock.md)
  - [table_tuple_fetch_row_version](../t/table_tuple_fetch_row_version.md)
  - [EvalPlanQual](../E/EvalPlanQual.md)
  - IsolationUsesXactSnapshot
  - TupIsNull
  - SnapshotAny
  - TUPLE_LOCK_FLAG_FIND_LAST_VERSION
- Called from (representative examples):
  - [ExecBRDeleteTriggersNew](../E/ExecBRDeleteTriggersNew.md)
  - [ExecARDeleteTriggers](../E/ExecARDeleteTriggers.md)
  - [ExecBRUpdateTriggersNew](../E/ExecBRUpdateTriggersNew.md)
  - [ExecARUpdateTriggers](../E/ExecARUpdateTriggers.md)

## Notes and Other Information
- The function returns true if a valid tuple was fetched, false if the tuple should be skipped
- Handles serialization failures appropriately based on isolation level
- Implements special error handling for triggered data change violations
- Uses different strategies depending on whether EPQ rechecking is required
- Critical for maintaining data consistency during trigger execution in concurrent scenarios
- The function is static and only used internally within the trigger system

## Simplified Source

```c
static bool
GetTupleForTrigger(EState *estate,
                   EPQState *epqstate,
                   ResultRelInfo *relinfo,
                   ItemPointer tid,
                   LockTupleMode lockmode,
                   TupleTableSlot *oldslot,
                   bool do_epq_recheck,
                   TupleTableSlot **epqslot,
                   TM_Result *tmresultp,
                   TM_FailureData *tmfdp)
{
    Relation relation = relinfo->ri_RelationDesc;

    if (epqslot != NULL) {
        // Complex path: lock tuple and handle concurrency
        TM_Result test;
        TM_FailureData tmfd;
        int lockflags = 0;

        *epqslot = NULL;

        // Set lock flags for snapshot isolation
        if (!IsolationUsesXactSnapshot()) {
            lockflags |= TUPLE_LOCK_FLAG_FIND_LAST_VERSION;
        }

        // Lock the tuple
        test = table_tuple_lock(relation, tid, estate->es_snapshot, oldslot,
                               estate->es_output_cid, lockmode, LockWaitBlock,
                               lockflags, &tmfd);

        // Return status to caller
        if (tmresultp) *tmresultp = test;
        if (tmfdp) *tmfdp = tmfd;

        switch (test) {
            case TM_SelfModified:
                // Check if tuple was modified by current command
                if (tmfd.cmax != estate->es_output_cid) {
                    ereport(ERROR, "tuple modified by triggered operation");
                }
                return false;  // Skip this tuple

            case TM_Ok:
                if (tmfd.traversed) {
                    // Tuple was updated, handle with EPQ if requested
                    if (do_epq_recheck) {
                        *epqslot = EvalPlanQual(epqstate, relation,
                                               relinfo->ri_RangeTableIndex, oldslot);
                        if (TupIsNull(*epqslot)) {
                            *epqslot = NULL;
                            return false;
                        }
                    } else {
                        if (tmresultp) *tmresultp = TM_Updated;
                        return false;
                    }
                }
                break;

            case TM_Updated:
            case TM_Deleted:
                // Handle serialization failures and deletions
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR, "serialization failure due to concurrent modification");
                }
                return false;

            case TM_Invisible:
            default:
                elog(ERROR, "unexpected tuple lock status: %u", test);
                return false;
        }
    } else {
        // Simple path: just fetch the tuple
        if (!table_tuple_fetch_row_version(relation, tid, SnapshotAny, oldslot)) {
            elog(ERROR, "failed to fetch tuple for trigger");
        }
    }

    return true;
}
```