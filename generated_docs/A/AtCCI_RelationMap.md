# AtCCI_RelationMap

## Location
[src/backend/utils/cache/relmapper.c:504-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L504-L540)

## Overview
Activates any "pending" relation map updates at CommandCounterIncrement time, making them visible to the current transaction.

## Definition

```c
void
AtCCI_RelationMap(void)
```
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

## Simplified Source

```c
// Simplified version of AtCCI_RelationMap
void AtCCI_RelationMap(void) {
    // Process pending shared catalog mapping updates
    if (pending_shared_updates.num_mappings != 0) {
        merge_map_updates(&active_shared_updates, &pending_shared_updates, true);
        pending_shared_updates.num_mappings = 0;  // Clear pending count
    }

    // Process pending local catalog mapping updates
    if (pending_local_updates.num_mappings != 0) {
        merge_map_updates(&active_local_updates, &pending_local_updates, true);
        pending_local_updates.num_mappings = 0;   // Clear pending count
    }
}
```

Key simplifications made:
- Added descriptive comments explaining the purpose of each section
- Clarified that one section handles shared catalogs, the other handles local catalogs
- Added inline comments explaining the reset of num_mappings counters
- The function is already quite simple, so minimal changes were needed to preserve the essential logic
- No complex conditions or error handling to simplify - the function has a straightforward control flow