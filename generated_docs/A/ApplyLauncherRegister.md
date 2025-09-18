# ApplyLauncherRegister

## Location
[src/backend/replication/logical/launcher.c:931-966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L931-L966)

## Overview
Registers a background worker process that runs the logical replication launcher in PostgreSQL.

## Definition
```c
void ApplyLauncherRegister(void)
```

## Detailed Description
This function sets up and registers a background worker process for the logical replication launcher. It configures the background worker with specific properties including shared memory access, database connection capabilities, and sets it to start after recovery is finished. The function includes safety checks to prevent registration during binary upgrades or when logical replication is disabled (max_logical_replication_workers == 0). The registered worker will execute the ApplyLauncherMain function and will automatically restart every 5 seconds if it crashes.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [BackgroundWorker](../B/BackgroundWorker.md)
  - BGWORKER_SHMEM_ACCESS
  - BGWORKER_BACKEND_DATABASE_CONNECTION
  - BgWorkerStart_RecoveryFinished
  - BGW_MAXLEN
  - [RegisterBackgroundWorker](../R/RegisterBackgroundWorker.md)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md)
  - LOGICALLAUNCHER_H

## Notes and Other Information
- Disabled during binary upgrades to prevent replication conflicts with copied data files
- The launcher is configured to restart automatically every 5 seconds if it fails
- Requires shared memory access and database connection capabilities
- Only starts after recovery has finished (BgWorkerStart_RecoveryFinished)
- Part of PostgreSQL's background worker infrastructure for managing logical replication
- The actual worker process runs the ApplyLauncherMain function
- Returns void (no return value)