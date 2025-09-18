# RestoreSlotFromDisk

## Location
[src/backend/replication/slot.c:2169-2404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L2169-L2404)

## Overview
Loads a single replication slot from disk into memory during startup, performing comprehensive validation, checksum verification, and configuration requirement checks.

## Definition


## Detailed Description
This function handles the complex process of restoring a replication slot from persistent storage into shared memory during PostgreSQL startup. The restoration process includes multiple validation stages:

1. **File Cleanup**: Removes any temporary state files from interrupted operations
2. **File Reading**: Opens and synchronizes the state file, then reads it in two phases (header first, then full content)
3. **Validation**: Verifies magic number, version, length, and checksum integrity
4. **Ephemeral Handling**: Removes non-persistent slots that shouldn't survive restarts
5. **Configuration Checks**: Validates that current server settings (wal_level, hot_standby) support the slot type
6. **Memory Restoration**: Finds an available slot in shared memory and initializes it with the persistent data

The function uses PANIC-level errors for corruption or configuration issues, as these represent fundamental problems that prevent safe database operation.

## Parameters
- : Name of the replication slot to restore from the pg_replslot directory

## Dependencies
- Functions called/Symbols referenced:
  - , , 
  - , 
  - , 
  -  (for REPLICATION_SLOT_RESTORE_SYNC and REPLICATION_SLOT_READ events)
  -  (for directory synchronization)
  - 
  - , , ,  (checksum operations)
  -  (for removing ephemeral slot directories)
  -  (for copying persistent data)
  -  (for setting inactive_since)
- Called from:
  -  (src/backend/replication/slot.c:1932)

## Notes and Other Information
- This is a static function used internally within the slot.c file
- Uses PANIC errors for data corruption or configuration issues that prevent safe startup
- Performs two-phase reading: constant-size header first, then variable-size content
- Validates magic number (SLOT_MAGIC), version (SLOT_VERSION), and length consistency
- Ephemeral slots (persistency != RS_PERSISTENT) are deleted rather than restored
- Enforces WAL level requirements: logical slots need WAL_LEVEL_LOGICAL, physical slots need WAL_LEVEL_REPLICA
- Special handling for standby mode: logical slots require hot_standby to be enabled
- Initializes in-memory state including effective_xmin, candidate values, and timing information
- Uses wait events for monitoring I/O operations during slot restoration
- Critical sections protect directory fsync operations from interruption