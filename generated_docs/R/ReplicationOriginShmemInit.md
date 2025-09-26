# ReplicationOriginShmemInit

## Location
[src/backend/replication/logical/origin.c:526-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L526-L572)

## Overview
Initializes the shared memory segment for replication origin state tracking, setting up control structures and synchronization primitives.

## Definition
```c
void ReplicationOriginShmemInit(void)
```

## Detailed Description
This function initializes the shared memory infrastructure for tracking replication origin states. It's called during PostgreSQL startup as part of the shared memory initialization process. The function performs the following key operations:

1. **Memory Allocation**: Uses ShmemInitStruct to allocate or attach to the shared memory segment named "ReplicationOriginState" with the size calculated by ReplicationOriginShmemSize()

2. **Control Structure Setup**: Establishes the global replication_states_ctl pointer and the convenience replication_states array pointer

3. **First-Time Initialization**: If this is the first process to initialize this shared memory (found == false):
   - Zeros out the entire allocated memory
   - Sets up the LWLock tranche ID for replication origin state locks
   - Initializes individual LWLocks for each replication state slot
   - Initializes condition variables for each replication state slot

The function gracefully handles the case where max_replication_slots is 0 by returning early without any initialization.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [ShmemInitStruct](../S/ShmemInitStruct.md): Allocates or attaches to named shared memory segment
  - [ReplicationOriginShmemSize](ReplicationOriginShmemSize.md): Calculates required memory size
  - `MemSet`: Zeros out memory (used only during first initialization)
  - `[LWLockInitialize](../L/LWLockInitialize.md)`: Initializes lightweight locks for each replication state
  - [ConditionVariableInit](../C/ConditionVariableInit.md): Initializes condition variables for coordination
  - `max_replication_slots`: Global configuration parameter
  - `LWTRANCHE_REPLICATION_ORIGIN_STATE`: Tranche ID constant for lock management
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md): During PostgreSQL startup shared memory initialization

## Notes and Other Information
- Only performs initialization if max_replication_slots > 0
- Uses the "found" parameter from ShmemInitStruct to determine if this is first-time initialization
- Sets up both LWLocks and condition variables for each state slot to support concurrent access and waiting
- The tranche ID allows PostgreSQL's lock monitoring and debugging tools to identify these locks
- Global variables replication_states_ctl and replication_states are set up for use throughout the backend
- Part of the critical startup path - must complete successfully for PostgreSQL to start when replication is configured