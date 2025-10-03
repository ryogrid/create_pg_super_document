# WalRcvShmemInit

## Location
[src/backend/replication/walreceiverfuncs.c:54-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiverfuncs.c#L54-L74)

## Overview
Allocates and initializes the shared memory structures required for WAL receiver operations.

## Definition

```c
void
WalRcvShmemInit(void)
```
## Detailed Description
This function is responsible for setting up the shared memory area used by the WAL receiver subsystem. It allocates a shared memory segment of the size determined by WalRcvShmemSize() and initializes the WalRcvData structure if this is the first process to access it. The initialization includes setting up synchronization primitives like condition variables and spin locks, as well as initializing atomic variables and setting the initial WAL receiver state to WALRCV_STOPPED.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [WalRcvData](WalRcvData.md)
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - [WalRcvShmemSize](WalRcvShmemSize.md)
  - MemSet
  - WALRCV_STOPPED
  - [ConditionVariableInit](../C/ConditionVariableInit.md)
  - SpinLockInit
  - [pg_atomic_init_u64](../p/pg_atomic_init_u64.md)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)

## Notes and Other Information
- Located in src/backend/replication/walreceiverfuncs.c:54-74
- Uses the standard PostgreSQL shared memory initialization pattern with ShmemInitStruct
- Only initializes the structure on first access (when found is false)
- Sets up critical synchronization primitives needed for multi-process coordination
- The global WalRcv pointer is assigned during this initialization
- Essential for proper operation of streaming replication functionality

## Simplified Source

```c
// Simplified version of WalRcvShmemInit
void WalRcvShmemInit(void) {
    bool found;

    // Allocate shared memory for WAL receiver control structure
    WalRcv = (WalRcvData *) ShmemInitStruct("Wal Receiver Ctl", WalRcvShmemSize(), &found);

    // Initialize structure only on first access
    if (!found) {
        // Clear the entire structure
        MemSet(WalRcv, 0, WalRcvShmemSize());

        // Set initial state to stopped
        WalRcv->walRcvState = WALRCV_STOPPED;

        // Initialize synchronization primitives
        ConditionVariableInit(&WalRcv->walRcvStoppedCV);
        SpinLockInit(&WalRcv->mutex);

        // Initialize atomic counter for write position
        pg_atomic_init_u64(&WalRcv->writtenUpto, 0);

        // Clear the latch pointer
        WalRcv->latch = NULL;
    }
}
```

Key simplifications made:
- Added descriptive comments for each major step
- Grouped related initialization operations logically
- Clarified the purpose of the `found` flag check
- Explained the role of each initialized component