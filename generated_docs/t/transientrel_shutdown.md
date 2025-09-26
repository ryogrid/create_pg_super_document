# transientrel_shutdown

## Location
[src/backend/commands/matview.c:520-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/matview.c#L520-L536)

## Overview
transientrel_shutdown is an executor shutdown callback function that finalizes bulk insert operations and cleanly closes the transient relation while maintaining necessary locks.

## Definition
static void transientrel_shutdown(DestReceiver *self)

## Detailed Description
This function serves as the shutdown callback for a DestReceiver that handles writing tuples to a transient relation. It performs the necessary cleanup operations after all tuples have been inserted, including freeing the bulk insert state, finalizing the bulk insert operation, and closing the transient relation. The function ensures proper resource cleanup while maintaining the relation lock until transaction commit, which is important for transactional consistency in materialized view operations.

## Parameters / Member Variables
- `self`: DestReceiver pointer cast to DR_transientrel containing the state for the transient relation to be shut down

## Dependencies
- Functions called/Symbols referenced:
  - [FreeBulkInsertState](../F/FreeBulkInsertState.md)
  - [table_finish_bulk_insert](table_finish_bulk_insert.md)
  - [table_close](table_close.md)
- Called from (representative examples):
  - [CreateTransientRelDestReceiver](../C/CreateTransientRelDestReceiver.md) (callback assignment)

## Notes and Other Information
- Frees the BulkInsertState to release memory and resources allocated for bulk insert optimization
- Calls table_finish_bulk_insert to ensure all pending writes are flushed and bulk insert state is properly finalized
- Closes the relation with NoLock, preserving the lock acquired during startup until transaction commit
- Sets the transientrel pointer to NULL to prevent accidental reuse after shutdown
- Part of the cleanup sequence in materialized view refresh operations where the transient relation served as temporary storage