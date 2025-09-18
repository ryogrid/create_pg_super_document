# CreateOrAttachShmemStructs

## Location
src/backend/storage/ipc/ipci.c: 281 - 368

## Overview
Initializes or attaches to shared memory data structures for all PostgreSQL subsystems in a systematic, dependency-aware order.

## Definition


## Detailed Description
CreateOrAttachShmemStructs is a comprehensive initialization function that sets up shared memory data structures for all major PostgreSQL subsystems. The function is designed to work in both scenarios: when the postmaster creates shared memory structures for the first time, and when child processes (particularly in EXEC_BACKEND mode) need to attach to existing structures.

The function follows a carefully orchestrated initialization sequence that respects inter-subsystem dependencies. It begins with fundamental infrastructure (LWLocks, shared memory index), then proceeds through transaction logging systems (WAL, CLOG, etc.), buffer management, locking subsystems, process management, inter-process communication, and finally specialized modules.

The initialization order is critical - for example, LWLocks must be initialized first as they're required by InitShmemIndex, and the shared memory index must be established before other subsystems can allocate their shared memory regions.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - CreateLWLocks (lightweight lock initialization)
  - [InitShmemIndex](../I/InitShmemIndex.md) (shared memory index setup)
  - dsm_shmem_init, DSMRegistryShmemInit (dynamic shared memory)
  - VarsupShmemInit, XLOGShmemInit, XLogPrefetchShmemInit, XLogRecoveryShmemInit (WAL subsystems)
  - [CLOGShmemInit](CLOGShmemInit.md), CommitTsShmemInit, SUBTRANSShmemInit, MultiXactShmemInit (transaction status)
  - InitBufferPool (buffer cache initialization)
  - InitLocks, InitPredicateLocks (lock management)
  - InitProcGlobal, CreateSharedProcArray, CreateSharedBackendStatus (process management)
  - Multiple other subsystem initialization functions
- Called from (representative examples):
  - [CreateSharedMemoryAndSemaphores](CreateSharedMemoryAndSemaphores.md) (postmaster startup)
  - [AttachSharedMemoryStructs](../A/AttachSharedMemoryStructs.md) (child process attachment)

## Notes and Other Information
- Static function - internal to ipci.c module
- Handles both creation (postmaster) and attachment (child process) scenarios
- Initialization order is dependency-driven and must be preserved
- Some subsystems check IsUnderPostmaster to determine creation vs. attachment mode
- Includes conditional initialization of InitProcGlobal only for postmaster
- Covers all major PostgreSQL subsystems: WAL, transactions, locks, processes, replication, statistics
- Essential for proper PostgreSQL inter-process communication and shared state management