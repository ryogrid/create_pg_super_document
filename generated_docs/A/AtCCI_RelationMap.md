# AtCCI_RelationMap

## Location
[src/backend/utils/cache/relmapper.c:504-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L504-L540)

## Overview
Activates any "pending" relation map updates at CommandCounterIncrement time, making them visible to the current transaction.

## Definition


## Detailed Description
The AtCCI_RelationMap function is responsible for activating pending relation mapping updates when a CommandCounterIncrement occurs. In PostgreSQL's relation mapping system, updates to the mapping between catalog OIDs and file numbers follow a visibility protocol similar to regular table updates - they become visible only at CommandCounterIncrement boundaries.

The function operates on two types of relation mappings:
1. **Shared mappings**: For shared catalogs that are visible across all databases
2. **Local mappings**: For database-specific catalogs

When called, it merges any pending updates into the corresponding active update structures by calling merge_map_updates for both shared and local mappings. After merging, it resets the pending update counters to zero, effectively "consuming" the pending updates.

This mechanism ensures that relation mapping changes follow PostgreSQL's MVCC semantics, where changes become visible at well-defined transaction boundaries.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [merge_map_updates](../m/merge_map_updates.md) (called twice - once for shared updates, once for local updates)
- Global variables accessed:
  - pending_shared_updates (static RelMapFile structure)
  - pending_local_updates (static RelMapFile structure) 
  - active_shared_updates (static RelMapFile structure)
  - active_local_updates (static RelMapFile structure)
- Called from (representative examples):
  - [AtCCI_LocalCache](AtCCI_LocalCache.md) (in src/backend/access/transam/xact.c)

## Notes and Other Information
- This function is part of PostgreSQL's relation mapping infrastructure that handles the special case of "mapped catalogs" like pg_class itself
- The function is called during CommandCounterIncrement processing to maintain transactional visibility semantics for relation mapping changes
- The pending/active distinction allows relation mapping updates to behave similarly to regular catalog updates in terms of when they become visible
- The function operates on static global variables that maintain the current state of relation mappings
- No error handling is present as the merge_map_updates function handles the actual work and any potential issues