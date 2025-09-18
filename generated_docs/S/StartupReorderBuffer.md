# StartupReorderBuffer

## Location
[src/backend/replication/logical/reorderbuffer.c:4784-4817](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4784-L4817)

## Overview
Cleans up all serialized (spilled) transaction data from replication slots after a PostgreSQL server restart or crash to ensure a clean state for logical replication.

## Definition


## Detailed Description
This function is called during PostgreSQL startup to perform cleanup of serialized reorder buffer data. When logical replication processes large transactions that exceed memory limits, they serialize (spill) the transaction data to disk in the pg_replslot directory. After a server restart or crash, this leftover spilled data needs to be cleaned up since it will be recreated when the respective replication slots are used again. The function iterates through all directories in pg_replslot, validates that they are legitimate replication slot directories, and removes all serialized transaction files (those starting with "xid-") from each slot directory.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - AllocateDir (opens pg_replslot directory)
  - ReadDir (reads directory entries)
  - [ReplicationSlotValidateName](../R/ReplicationSlotValidateName.md) (validates slot directory names)
  - [ReorderBufferCleanupSerializedTXNs](../R/ReorderBufferCleanupSerializedTXNs.md) (removes spilled files from individual slots)
  - FreeDir (closes directory handle)
  - strcmp (string comparison for directory entry filtering)
- Called from (representative examples):
  - [StartupXLOG](StartupXLOG.md) (during WAL recovery startup process)

## Notes and Other Information
- This is a cleanup function specifically designed for startup scenarios
- Only processes directories that pass replication slot name validation
- Skips standard directory entries ("." and "..")
- The cleanup is necessary because spilled transaction data becomes invalid after a restart
- Part of PostgreSQL's logical replication recovery mechanism
- Ensures that replication slots start with a clean state after server recovery
- Uses DEBUG2 log level for slot name validation, keeping startup logs clean under normal conditions