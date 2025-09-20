# CreateSlotOnDisk

## Location
[src/backend/replication/slot.c:1953-2013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1953-L2013)

## Overview
Creates a replication slot's persistent directory structure on disk using atomic rename operations to ensure crash safety.

## Definition

```c
struct stat st;
```
## Detailed Description
This function creates the on-disk representation of a replication slot using a two-phase approach to ensure atomic creation:

1. **Temporary Creation**: Creates a temporary directory with a .tmp suffix
2. **State Writing**: Saves the slot's state to the temporary directory
3. **Atomic Rename**: Renames the temporary directory to the final name

The function implements crash safety through several mechanisms:
- Uses temporary directories to avoid partial slot creation
- Performs fsync operations at critical points
- Uses critical sections to ensure restart on failure during final steps
- Cleans up any existing temporary directories from previous failed attempts

The atomic rename operation ensures that the slot either exists completely or not at all, preventing corruption from system crashes during slot creation.

## Parameters
- : Pointer to the ReplicationSlot structure containing slot data and metadata to be persisted to disk

## Dependencies
- Functions called/Symbols referenced:
  -  (for path construction)
  -  (for checking existing temp directories)
  -  (macro for directory type checking)
  -  (for cleaning up existing temp directories)
  -  (for creating the temporary directory)
  -  (for ensuring data persistence)
  -  (for writing slot state)
  -  (for atomic directory rename)
  -  (for critical section protection)
- Called from:
  -  (src/backend/replication/slot.c:418)

## Notes and Other Information
- This is a static function, only used within the slot.c file
- No io_in_progress_lock needed since the slot is not yet visible to other processes
- Uses .tmp suffix for temporary directories to indicate incomplete operations
- Critical section ensures server restart if fsync operations fail during final steps
- [Path](../P/Path.md) format follows: "pg_replslot/[slot_name]" and "pg_replslot/[slot_name].tmp"
- Error reporting uses ERROR level, which will abort the current transaction
- Implements crash-safe slot creation through atomic filesystem operations