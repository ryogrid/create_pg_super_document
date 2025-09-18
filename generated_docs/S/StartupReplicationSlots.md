# StartupReplicationSlots

## Location
[src/backend/replication/slot.c:1892-1952](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1892-L1952)

## Overview
Loads all replication slots from disk into memory during server startup, performing cleanup of temporary directories and computing required xmin and LSN values.

## Definition


## Detailed Description
This function initializes the replication slot system during PostgreSQL server startup by scanning the pg_replslot directory and restoring all valid replication slots from disk into shared memory. The function performs several critical operations:

1. Iterates through all entries in the pg_replslot directory
2. Identifies and cleans up temporary directories (ending with .tmp) that indicate interrupted slot operations
3. Restores valid slot directories by calling RestoreSlotFromDisk
4. Computes the required xmin and LSN values across all restored slots

The function must run before crash recovery begins to ensure that replication slots are properly loaded and their requirements are known to the recovery process.

## Parameters
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  (with DEBUG1 level)
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - , 
- Called from:
  -  (src/backend/access/transam/xlog.c:5556)

## Notes and Other Information
- Must be called during server startup before crash recovery begins
- Handles cleanup of interrupted slot operations by removing .tmp directories
- Skips non-directory entries and standard directory entries (. and ..)
- Uses DEBUG1 logging level for directory type determination
- After restoring all slots, computes global xmin and LSN requirements for the replication system
- Early exits if max_replication_slots is 0 or negative
- Performs fsync on the pg_replslot directory after cleaning up temporary directories
- Critical for maintaining replication slot consistency across server restarts