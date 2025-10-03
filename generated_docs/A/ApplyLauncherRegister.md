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

## Simplified Source

```c
// Simplified version of ApplyLauncherRegister
void ApplyLauncherRegister(void) {
    BackgroundWorker bgw;

    // Skip registration if logical replication disabled or during binary upgrade
    if (max_logical_replication_workers == 0 || IsBinaryUpgrade)
        return;

    // Initialize background worker structure
    memset(&bgw, 0, sizeof(bgw));

    // Configure worker properties
    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
    bgw.bgw_restart_time = 5;  // Restart every 5 seconds if crashed

    // Set worker identification and entry point
    snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
    snprintf(bgw.bgw_function_name, BGW_MAXLEN, "ApplyLauncherMain");
    snprintf(bgw.bgw_name, BGW_MAXLEN, "logical replication launcher");
    snprintf(bgw.bgw_type, BGW_MAXLEN, "logical replication launcher");

    // Register the background worker with the system
    RegisterBackgroundWorker(&bgw);
}
```

Key simplifications made:
- Consolidated comments to focus on main logic flow
- Grouped related configuration steps together
- Emphasized the core purpose: conditional registration of a background worker
- Maintained all essential functionality while improving readability