# BaseInit

## Location
[src/backend/utils/init/postinit.c:647-737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L647-L737)

## Overview
BaseInit performs early initialization of a PostgreSQL backend process that is required before InitPostgres, used by both regular backends and auxiliary processes like background writer.

## Definition


## Detailed Description
BaseInit is a fundamental initialization function that sets up essential subsystems required for any PostgreSQL backend process to function. It is designed to be called early in the process lifecycle, even before InitPostgres, and is shared between regular backends under the postmaster and auxiliary processes that may never call InitPostgres at all.

The function initializes critical low-level subsystems in a carefully ordered sequence to ensure proper dependencies are met. It sets up file access capabilities, statistics reporting infrastructure, storage management, buffer management, temporary file handling, WAL record construction capabilities, and replication slot functionality.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [DebugFileOpen](../D/DebugFileOpen.md)
  - InitFileAccess
  - [pgstat_initialize](../p/pgstat_initialize.md)
  - [InitSync](../I/InitSync.md)
  - [smgrinit](../s/smgrinit.md)
  - [InitBufferPoolAccess](../I/InitBufferPoolAccess.md)
  - InitTemporaryFileAccess
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