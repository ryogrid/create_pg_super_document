# pgstat_prep_pending_entry

## Location
[src/backend/utils/activity/pgstat.c:1107-1144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1107-L1144)

## Overview
Prepares and returns a statistics entry reference for accumulating pending statistics data, creating the necessary infrastructure and memory allocations if not already present.

## Definition


## Detailed Description
This function serves as the primary interface for setting up pending statistics infrastructure for a specific statistics object. It ensures that all necessary components are in place for accumulating statistics data before it gets flushed to shared memory. The function handles lazy initialization of the pending statistics memory context and manages the allocation of pending data structures for specific statistics entries.

The function validates that the requested statistics kind supports pending data operations by checking for a flush callback. It creates the pending statistics memory context on first use, retrieves or creates an entry reference for the specified object, and allocates pending data storage if not already present. The pending entry is added to the global pending list for later processing during statistics flushing operations.

## Parameters / Member Variables
- : The statistics kind (PgStat_Kind) identifying the type of statistics being prepared
- : The database OID associated with the statistics object (can be InvalidOid for global objects)
- : The object OID for the specific statistics entry being prepared
- : Optional output parameter (bool *) that will be set to true if a new entry reference was created, false if an existing one was reused

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_get_kind_info](pgstat_get_kind_info.md)
  - AllocSetContextCreate
  - pgstat_get_entry_ref
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - ALLOCSET_SMALL_SIZES
- Called from (representative examples):
  - [pgstat_prep_database_pending](pgstat_prep_database_pending.md)
  - [pgstat_init_function_usage](pgstat_init_function_usage.md)
  - [pgstat_prep_relation_pending](pgstat_prep_relation_pending.md)
  - [pgstat_report_subscription_error](pgstat_report_subscription_error.md)

## Notes and Other Information
- Creates the PgStat Pending memory context using TopMemoryContext as parent for proper lifetime management
- Uses ALLOCSET_SMALL_SIZES allocation strategy optimized for small, frequent allocations
- Validates that the statistics kind has a registered flush_pending_cb before proceeding
- Allocates zero-initialized memory for pending data to ensure clean starting state
- Maintains a doubly-linked list of all pending entries for efficient traversal during flush operations
- The function is safe to call multiple times for the same object - it will reuse existing structures
- Returns a valid PgStat_EntryRef pointer that can be used immediately for statistics accumulation