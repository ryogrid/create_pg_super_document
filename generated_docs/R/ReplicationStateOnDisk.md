# ReplicationStateOnDisk

## Location
[src/backend/replication/logical/origin.c:139-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L139-L143)

## Overview
ReplicationStateOnDisk is a simplified structure representing the persistent, on-disk version of replication state information for logical replication origins.

## Definition
```c
typedef struct ReplicationStateOnDisk
{
    RepOriginId roident;
    XLogRecPtr remote_lsn;
} ReplicationStateOnDisk;
```

## Detailed Description
The ReplicationStateOnDisk structure is a streamlined version of ReplicationState designed specifically for persistence to disk during checkpoints and recovery operations. Unlike the in-memory ReplicationState structure, this disk version contains only the essential information needed to restore replication state after a server restart: the replication origin identifier and the remote LSN position.

This structure excludes the synchronization primitives (locks and condition variables) and process-specific information (acquired_by, local_lsn) that are only meaningful during runtime. The separation between in-memory and on-disk representations allows for efficient storage while maintaining the full feature set in memory.

## Parameters / Member Variables
- `roident`: Local identifier for the remote replication origin node
- `remote_lsn`: XLog location of the latest commit received from the remote side

## Dependencies
- Functions called/Symbols referenced:
  - RepOriginId (replication origin identifier type)
- Called from (representative examples):
  - CheckPointReplicationOrigin
  - StartupReplicationOrigin

## Notes and Other Information
- This structure is used during checkpoint operations to persist replication progress to disk
- During server startup/recovery, this structure is read from disk to restore replication state
- The simplified structure reduces disk I/O overhead while preserving essential replication tracking information
- The local_lsn field from ReplicationState is not persisted since it can be derived during recovery
- Synchronization fields are not needed on disk as they are reconstructed during shared memory initialization