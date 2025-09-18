# GetTupleForTrigger

## Location
src/backend/commands/trigger.c: 3371 - 3508

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
  - table_tuple_lock
  - table_tuple_fetch_row_version
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