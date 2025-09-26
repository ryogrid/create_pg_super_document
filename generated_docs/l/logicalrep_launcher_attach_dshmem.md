# logicalrep_launcher_attach_dshmem

## Location
[src/backend/replication/logical/launcher.c:1002-1045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L1002-L1045)

## Overview
Initializes or attaches to the dynamic shared hash table that stores worker last-start times for logical replication, ensuring thread-safe access to worker timing information.

## Definition
```c
static void logicalrep_launcher_attach_dshmem(void)
```

## Detailed Description
This function manages the initialization and attachment to a dynamic shared hash table (dshash) that tracks the last start times of logical replication workers. It implements a lazy initialization pattern with proper synchronization to ensure that only one process creates the shared table while allowing multiple processes to attach to it safely.

The function uses a two-phase check: first a quick exit if the table is already initialized locally, then an exclusive lock-protected section to handle the actual creation or attachment. The dynamic shared memory area (DSA) and hash table are created in the TopMemoryContext to ensure persistence across memory context resets.

When creating a new table, it sets up both the DSA and dshash components, pins them to prevent premature cleanup, and stores the handles in the LogicalRepCtx shared memory structure. When attaching to an existing table, it retrieves the handles from shared memory and attaches to the pre-existing structures.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - dsa_create
  - [dsa_pin](../d/dsa_pin.md)
  - [dsa_pin_mapping](../d/dsa_pin_mapping.md)
  - [dshash_create](../d/dshash_create.md)
  - [dsa_get_handle](../d/dsa_get_handle.md)
  - [dshash_get_hash_table_handle](../d/dshash_get_hash_table_handle.md)
  - [dsa_attach](../d/dsa_attach.md)
  - [dshash_attach](../d/dshash_attach.md)
  - DSHASH_HANDLE_INVALID
  - LWTRANCHE_LAUNCHER_DSA
- Called from:
  - [ApplyLauncherSetWorkerStartTime](../A/ApplyLauncherSetWorkerStartTime.md)
  - [ApplyLauncherGetWorkerStartTime](../A/ApplyLauncherGetWorkerStartTime.md)
  - [ApplyLauncherForgetWorkerStartTime](../A/ApplyLauncherForgetWorkerStartTime.md)

## Notes and Other Information
- This is a static function within the launcher.c file, indicating it's only used internally by the logical replication launcher module
- Uses LogicalRepWorkerLock in exclusive mode to prevent race conditions during table creation/attachment
- The function implements the common PostgreSQL pattern of lazy initialization with shared memory structures
- Memory allocations are performed in TopMemoryContext to ensure they persist beyond the current operation
- The dual-handle approach (DSA handle and dshash handle) allows for proper reconstruction of the shared structures in different processes