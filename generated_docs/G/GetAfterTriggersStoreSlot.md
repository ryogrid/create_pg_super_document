# GetAfterTriggersStoreSlot

## Location
src/backend/commands/trigger.c: 4920 - 4968

## Overview
Returns a TupleTableSlot suitable for holding tuples that will be stored in AfterTriggersTableData's transition table tuplestores, creating the slot lazily if it doesn't exist.

## Definition
```c
static TupleTableSlot *GetAfterTriggersStoreSlot(AfterTriggersTableData *table,
                                                  TupleDesc tupdesc)
```

## Detailed Description
This function provides lazy initialization of tuple table slots used for PostgreSQL's transition table functionality in trigger processing. When called for the first time on a given AfterTriggersTableData structure, it creates a new TupleTableSlot configured for virtual tuple operations.

The function implements careful memory management by copying the provided tuple descriptor to ensure it has the appropriate lifetime. The slot is created in CurTransactionContext to ensure it persists until the end of the subtransaction, which is sufficient since it only needs to last until AfterTriggerEndQuery. The slot will be automatically freed by AfterTriggerFreeQuery.

The function uses TTSOpsVirtual for the slot operations, which is appropriate for transition table storage where tuples may be constructed in memory rather than read from disk.

## Parameters / Member Variables
- `table`: Pointer to AfterTriggersTableData structure that will own the slot
- `tupdesc`: TupleDesc describing the structure of tuples that will be stored in the slot

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [TransitionTableAddTuple](../T/TransitionTableAddTuple.md)
  - [AfterTriggersTableData](../A/AfterTriggersTableData.md) (within trigger.c)

## Notes and Other Information
- Returns the existing storeslot if already created, or creates and returns a new one
- Performs lazy initialization - the slot is only created when first requested
- Creates a copy of the provided tupdesc to ensure proper memory lifetime management
- Uses TTSOpsVirtual operations for efficient in-memory tuple handling
- Allocated in CurTransactionContext for appropriate lifetime management
- Part of PostgreSQL's transition table infrastructure used by AFTER triggers to access OLD/NEW row data
- The slot will be automatically cleaned up by AfterTriggerFreeQuery when the query completes