# SISeg

## Location
src/backend/storage/ipc/sinvaladt.c: 166 - 197

## Overview
SISeg represents the main shared memory segment structure for PostgreSQL's shared cache invalidation system, containing the circular message buffer and per-backend state tracking for coordinating cache invalidation across all backend processes.

## Definition
```c
typedef struct SISeg
{
    int                     minMsgNum;      /* oldest message still needed */
    int                     maxMsgNum;      /* next message number to be assigned */
    int                     nextThreshold;  /* # of messages to call SICleanupQueue */
    slock_t                 msgnumLock;     /* spinlock protecting maxMsgNum */
    SharedInvalidationMessage buffer[MAXNUMMESSAGES];  /* circular buffer */
    int                     numProcs;       /* number of active backend slots */
    int                    *pgprocnos;      /* dense array of active proc indexes */
    ProcState               procState[FLEXIBLE_ARRAY_MEMBER];  /* per-backend state */
} SISeg;
```

## Detailed Description
SISeg is the central data structure for PostgreSQL's shared invalidation system, residing in shared memory to coordinate cache invalidation messages between all backend processes. It manages a circular buffer of invalidation messages and maintains state information for each participating backend process.

The structure uses a message numbering scheme where messages are assigned sequential numbers, allowing backends to track their position in the message stream. The circular buffer efficiently stores invalidation messages, while the cleanup mechanism ensures old, no-longer-needed messages are purged to prevent buffer overflow.

The per-backend state tracking includes both a sparse array (procState) indexed by process number and a dense array (pgprocnos) of active process indexes. This dual representation allows for efficient iteration over active processes while maintaining direct access by process number.

## Parameters / Member Variables
- `minMsgNum`: Sequence number of the oldest invalidation message still needed by any backend
- `maxMsgNum`: Next message sequence number to be assigned to new invalidation messages
- `nextThreshold`: Number of messages threshold for triggering SICleanupQueue cleanup routine
- `msgnumLock`: Spinlock protecting concurrent access to maxMsgNum during message insertion
- `buffer`: Circular buffer array holding MAXNUMMESSAGES invalidation messages
- `numProcs`: Count of currently active backend processes participating in invalidation
- `pgprocnos`: Dense array containing indexes of active backend processes for efficient scanning
- `procState`: Flexible array of ProcState structures, one per potential backend, indexed by process number

## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](../s/slock_t.md)
  - MAXNUMMESSAGES
  - SharedInvalidationMessage
  - [ProcState](../P/ProcState.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [CreateSharedInvalidationState](../C/CreateSharedInvalidationState.md)
  - [SharedInvalBackendInit](SharedInvalBackendInit.md)
  - [CleanupInvalidationState](../C/CleanupInvalidationState.md)
  - [SIInsertDataEntries](SIInsertDataEntries.md)
  - SIGetDataEntries
  - SICleanupQueue
  - SIResetAll

## Notes and Other Information
- This structure resides in shared memory and must be accessed with appropriate locking protocols
- The circular buffer design allows efficient message storage with automatic wraparound when the buffer fills
- The pgprocnos array is maintained separately from ProcArrayStruct->pgprocnos to avoid contention on ProcArrayLock and to track only processes participating in shared invalidation
- The msgnumLock spinlock is specifically used to protect maxMsgNum updates during concurrent message insertions
- The flexible array member for procState allows the structure size to be determined at runtime based on MaxBackends configuration
- Cleanup operations are triggered by nextThreshold to maintain buffer efficiency and prevent overflow
- The dual indexing scheme (sparse procState array and dense pgprocnos array) optimizes both direct access and iteration patterns