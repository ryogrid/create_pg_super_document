# ParallelSlotsGetIdle

## Location
[src/fe_utils/parallel_slot.c:371-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/parallel_slot.c#L371-L427)

## Overview
ParallelSlotsGetIdle is a public function that returns an available parallel slot ready to execute a command, implementing a sophisticated slot allocation strategy with connection reuse and blocking capabilities.

## Definition

```c
ParallelSlot *
ParallelSlotsGetIdle(ParallelSlotArray *sa, const char *dbname)
```
## Detailed Description
This function implements a four-tier strategy for obtaining an idle parallel slot. It first attempts to reuse an existing connection to the desired database, then tries to use an unconnected slot, followed by disconnecting and reconnecting a slot connected to a different database. As a last resort, it blocks until a busy slot becomes available. The function ensures optimal connection reuse while providing the flexibility to connect to different databases as needed. It includes comprehensive error handling and returns NULL if no slot can be obtained or if fatal errors occur.

## Parameters / Member Variables
- : Pointer to the ParallelSlotArray containing the slots to manage
- : Optional database name to connect to; can be NULL to use default database

## Dependencies
- Functions called/Symbols referenced:
  - [find_matching_idle_slot](../f/find_matching_idle_slot.md) (finds slot already connected to desired database)
  - [find_unconnected_slot](../f/find_unconnected_slot.md) (finds slot with no database connection)
  - [find_any_idle_slot](../f/find_any_idle_slot.md) (finds any available idle slot)
  - [connect_slot](../c/connect_slot.md) (establishes new database connection for slot)
  - [disconnectDatabase](../d/disconnectDatabase.md) (closes existing database connection)
  - [wait_on_slots](../w/wait_on_slots.md) (blocks until slots become available)
- Called from (representative examples):
  - [main](../m/main.md) (pg_amcheck)
  - [reindex_one_database](../r/reindex_one_database.md) (reindexdb)
  - [vacuum_one_database](../v/vacuum_one_database.md) (vacuumdb)
  - [ParallelSlotClearHandler](ParallelSlotClearHandler.md)

## Notes and Other Information
- Returns a pointer to an available ParallelSlot, or NULL on error
- Implements optimal connection reuse strategy to minimize connection overhead
- Marks returned slots as in-use (inUse = true) before returning
- Uses infinite loop with blocking wait as fallback to ensure slot availability
- Handles database switching by disconnecting and reconnecting when necessary
- Executes initialization commands on newly created connections if configured
- Critical for efficient parallel processing in PostgreSQL client tools
- Function is part of the public API (not static) for use across different PostgreSQL utilities