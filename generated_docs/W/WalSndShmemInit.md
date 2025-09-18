# WalSndShmemInit

## Location
src/backend/replication/walsender.c: 3663 - 3707

## Overview
WalSndShmemInit initializes the shared memory structures needed for WAL (Write-Ahead Log) sender processes in PostgreSQL's streaming replication system.

## Definition
```c
void WalSndShmemInit(void)
```

## Detailed Description
This function allocates and initializes the shared memory control structure for WAL senders during PostgreSQL server startup. It performs the following key operations:

1. **Shared Memory Allocation**: Allocates shared memory for the WalSndCtl structure using ShmemInitStruct
2. **First-time Initialization**: If this is the first time the structure is being created (not found in existing shared memory), it:
   - Zeroes out the entire structure using MemSet
   - Initializes synchronous replication queues for each wait mode
   - Initializes spin locks for each WAL sender slot
   - Initializes condition variables for WAL flush, replay, and confirmation events

The function ensures that the WAL sender infrastructure is properly set up in shared memory before any replication connections are established.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ShmemInitStruct (shared memory structure initialization)
  - WalSndShmemSize (calculates required shared memory size)
  - MemSet (memory zeroing)
  - dlist_init (initializes doubly-linked lists for sync rep queues)
  - SpinLockInit (initializes spin locks for WAL sender mutexes)
  - ConditionVariableInit (initializes condition variables)
- Called from (representative examples):
  - CreateOrAttachShmemStructs (during server startup)

## Notes and Other Information
- This function is called once during PostgreSQL server startup as part of shared memory initialization
- The WalSndCtl structure manages up to max_wal_senders concurrent WAL sender processes
- Each WAL sender slot gets its own spin lock for thread-safe access
- Three condition variables are initialized for coordinating WAL flush, replay, and confirmation events
- The synchronous replication queues are organized by wait mode (NUM_SYNC_REP_WAIT_MODE different modes)
- Memory layout is critical for multi-process access in PostgreSQL's shared memory architecture