# ApplyLauncherShmemInit

## Location
src/backend/replication/logical/launcher.c: 967 - 1001

## Overview
Allocates and initializes the shared memory structures needed for PostgreSQL's logical replication launcher subsystem.

## Definition
```c
void ApplyLauncherShmemInit(void)
```

## Detailed Description
This function sets up the shared memory infrastructure for the logical replication launcher. It uses the PostgreSQL shared memory management system to either find an existing "Logical Replication Launcher Data" segment or create a new one using the size calculated by ApplyLauncherShmemSize(). When creating new shared memory (when 'found' is false), it initializes the LogicalRepCtx structure, sets up invalid handles for DSA and DSHASH, and initializes all worker slots with proper spin locks. Each worker slot gets its memory zeroed and its relation mutex (relmutex) spin lock initialized.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - LogicalRepCtxStruct
  - ShmemInitStruct
  - ApplyLauncherShmemSize
  - DSA_HANDLE_INVALID
  - DSHASH_HANDLE_INVALID
  - LogicalRepWorker
  - SpinLockInit
- Called from (representative examples):
  - CreateOrAttachShmemStructs
  - LOGICALLAUNCHER_H

## Notes and Other Information
- Part of PostgreSQL's shared memory initialization during startup
- Only initializes the structures when creating new shared memory (not when attaching to existing)
- Sets up invalid handles for Dynamic Shared Areas (DSA) and Dynamic Shared Hash tables (DSHASH)
- Initializes spin locks for each worker slot to ensure thread-safe access to worker data
- The function works with the max_logical_replication_workers configuration parameter
- Creates properly aligned and initialized shared memory structures for logical replication workers
- Returns void (no return value)