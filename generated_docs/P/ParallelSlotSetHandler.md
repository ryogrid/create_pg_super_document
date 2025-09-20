# ParallelSlotSetHandler

## Location
[src/include/fe_utils/parallel_slot.h:47-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/parallel_slot.h#L47-L54)

## Overview
Sets a result handler callback function and its context for a parallel slot, enabling custom processing of query results when the slot completes its operation.

## Definition

```c
static inline void
ParallelSlotSetHandler(ParallelSlot *slot, ParallelSlotResultHandler handler,
					   void *context)
```
## Detailed Description
ParallelSlotSetHandler is an inline function that assigns a result handler callback function and its associated context data to a ParallelSlot. This mechanism allows different operations to register custom processing logic that will be invoked when the parallel operation completes. The handler function follows the ParallelSlotResultHandler signature, which receives a PGresult, PGconn, and the context data as parameters. This design enables flexible, operation-specific result processing in PostgreSQL's parallel execution framework for client tools.

## Parameters / Member Variables
- `slot`: Pointer to the ParallelSlot structure that will receive the handler configuration
- `handler`: Function pointer of type ParallelSlotResultHandler that will be called to process results
- `context`: Void pointer to arbitrary data that will be passed to the handler function when invoked

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelSlot](ParallelSlot.md) (struct type)
  - ParallelSlotResultHandler (typedef)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_amcheck.c)
  - [reindex_one_database](../r/reindex_one_database.md) (in reindexdb.c)  
  - [vacuum_one_database](../v/vacuum_one_database.md) (in vacuumdb.c)

## Notes and Other Information
- This is an inline function defined in parallel_slot.h for performance
- The handler and context are stored in the slot->handler and slot->handler_context fields respectively
- The handler function should return bool to indicate success/failure of result processing
- Used extensively in PostgreSQL client tools like pg_amcheck, reindexdb, and vacuumdb for parallel operations
- The context parameter allows passing operation-specific data to the handler without global variables