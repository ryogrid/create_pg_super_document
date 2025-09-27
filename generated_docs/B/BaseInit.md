# BaseInit

## Location
[src/backend/utils/init/postinit.c:647-737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L647-L737)

## Overview
BaseInit performs early initialization of a PostgreSQL backend process that is required before InitPostgres, used by both regular backends and auxiliary processes like background writer.

## Definition

```c
struction, in case we ever
	 * try to insert XLOG.
	 */
	InitXLogInsert();
```
## Detailed Description
BaseInit is a fundamental initialization function that sets up essential subsystems required for any PostgreSQL backend process to function. It is designed to be called early in the process lifecycle, even before InitPostgres, and is shared between regular backends under the postmaster and auxiliary processes that may never call InitPostgres at all.

The function initializes critical low-level subsystems in a carefully ordered sequence to ensure proper dependencies are met. It sets up file access capabilities, statistics reporting infrastructure, storage management, buffer management, temporary file handling, WAL record construction capabilities, and replication slot functionality.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [DebugFileOpen](../D/DebugFileOpen.md)
  - [InitFileAccess](../I/InitFileAccess.md)
  - [pgstat_initialize](../p/pgstat_initialize.md)
  - [InitSync](../I/InitSync.md)
  - [smgrinit](../s/smgrinit.md)
  - [InitBufferPoolAccess](../I/InitBufferPoolAccess.md)
  - [InitTemporaryFileAccess](../I/InitTemporaryFileAccess.md)
  - [InitXLogInsert](../I/InitXLogInsert.md)
  - [ReplicationSlotInitialize](../R/ReplicationSlotInitialize.md)
- Called from (representative examples):
  - [BootstrapModeMain](BootstrapModeMain.md)
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md)
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)
  - [BackgroundWorkerMain](BackgroundWorkerMain.md)
  - [ReplSlotSyncWorkerMain](../R/ReplSlotSyncWorkerMain.md)
  - [PostgresMain](../P/PostgresMain.md)

## Notes and Other Information
- The function assumes MyProc != NULL and asserts this condition
- Initialization order is critical - statistics reporting is initialized early to ensure its shutdown callback runs after other subsystems
- Temporary file access is initialized after pgstat to enable proper statistics reporting for temporary files
- Replication slots are initialized after pgstat to allow ephemeral slot cleanup to trigger stats reporting
- This function is essential for both regular backend processes and auxiliary processes, making it a cornerstone of PostgreSQL's process initialization architecture

## Simplified Source

```c
// Simplified version of BaseInit
void BaseInit(void) {
    // Ensure process structure is initialized
    Assert(MyProc != NULL);

    // Step 1: Initialize file I/O capabilities
    DebugFileOpen();
    InitFileAccess();

    // Step 2: Set up statistics reporting early for proper shutdown order
    pgstat_initialize();

    // Step 3: Initialize storage and buffer management
    InitSync();
    smgrinit();
    InitBufferPoolAccess();

    // Step 4: Initialize temporary file access after stats
    InitTemporaryFileAccess();

    // Step 5: Set up WAL record construction capabilities
    InitXLogInsert();

    // Step 6: Initialize replication slots after stats for proper cleanup
    ReplicationSlotInitialize();
}
```

Key simplifications made:
- Removed detailed comment blocks for clarity
- Consolidated initialization steps into logical groups
- Added step-by-step comments explaining the initialization sequence
- Preserved the critical assertion and initialization order
- Focused on the main execution path without auxiliary details