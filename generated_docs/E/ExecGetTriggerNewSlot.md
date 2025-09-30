# ExecGetTriggerNewSlot

## Location
[src/backend/executor/execUtils.c:1160-1181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L1160-L1181)

## Overview
Returns a tuple slot for storing the NEW tuple values in trigger processing, creating it lazily if it doesn't already exist.

## Definition

```c
TupleTableSlot *
ExecGetTriggerNewSlot(EState *estate, ResultRelInfo *relInfo)
```
## Detailed Description
This function provides access to a specialized tuple slot used during trigger execution to hold the "NEW" version of a tuple (i.e., the tuple values after an INSERT or UPDATE operation). Like its counterpart ExecGetTriggerOldSlot, it implements lazy initialization - the slot is only created when first requested and stored in the ResultRelInfo structure for subsequent reuse.

The NEW slot is crucial for trigger processing, particularly for INSERT and UPDATE operations where triggers need access to the new tuple values being inserted or the updated values. This function ensures that the NEW slot is properly initialized with the correct tuple descriptor and table access methods for the relation.

The slot is allocated in the query's memory context to ensure proper lifetime management throughout query execution.

## Parameters / Member Variables
- : The executor state containing query execution context and memory management information
- : Result relation info structure that maintains trigger-related tuple slots and relation metadata

## Dependencies
- Functions called/Symbols referenced:
  -  (creates and initializes a new tuple slot)
  -  (gets appropriate slot callback functions for the table)
- Called from (representative examples):
  -  (src/backend/commands/trigger.c:4495, 4513)
  -  (src/backend/commands/trigger.c:6381)
  -  (src/include/executor/executor.h:615)

## Notes and Other Information
- Uses lazy initialization pattern - slot is only created when first accessed
- The slot is stored in  for reuse across multiple trigger invocations
- Memory context is temporarily switched to  to ensure proper lifetime management
- Primarily used in INSERT and UPDATE triggers where the new tuple values need to be accessible
- Complementary to ExecGetTriggerOldSlot, together they provide complete OLD/NEW tuple access for triggers
- The implementation is nearly identical to ExecGetTriggerOldSlot, differing only in the slot field used (ri_TrigNewSlot vs ri_TrigOldSlot)
- Part of PostgreSQL's trigger infrastructure supporting BEFORE, AFTER, and INSTEAD OF triggers
- The tuple descriptor and slot callbacks are obtained from the relation to ensure compatibility with the table's storage format

## Simplified Source

```c
TupleTableSlot *ExecGetTriggerNewSlot(EState *estate, ResultRelInfo *relInfo) {
    // Create NEW slot if not already initialized
    if (relInfo->ri_TrigNewSlot == NULL) {
        Relation rel = relInfo->ri_RelationDesc;

        // Switch to query context for proper slot lifetime
        MemoryContext oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

        // Initialize slot with relation's tuple descriptor and callbacks
        relInfo->ri_TrigNewSlot =
            ExecInitExtraTupleSlot(estate, RelationGetDescr(rel),
                                   table_slot_callbacks(rel));

        MemoryContextSwitchTo(oldcontext);
    }

    return relInfo->ri_TrigNewSlot;
}
```