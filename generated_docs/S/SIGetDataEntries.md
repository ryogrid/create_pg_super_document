# SIGetDataEntries

## Location
[src/backend/storage/ipc/sinvaladt.c:473-576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/sinvaladt.c#L473-L576)

## Overview
Retrieves the next shared invalidation message(s) for the current backend from the shared invalidation queue, providing a mechanism for backends to receive cache invalidation notifications.

## Definition

```c
int
SIGetDataEntries(SharedInvalidationMessage *data, int datasize)
```
## Detailed Description
SIGetDataEntries is a core function in PostgreSQL's shared invalidation system that allows individual backends to retrieve pending invalidation messages from the shared memory buffer. The function operates in a lock-safe manner, using shared locks to allow multiple backends to read messages concurrently while preventing conflicts with message insertion operations.

The function implements an optimized reading strategy with an initial unlocked check for performance, followed by proper locking when messages are available. It handles three distinct scenarios: no messages available (returns 0), normal message retrieval (returns count > 0), or reset condition (returns -1).

The implementation carefully manages the backend's state tracking, including message counters and signaling flags, to ensure reliable message delivery without loss or duplication. It can run in parallel with other instances serving different backends and with SIInsertDataEntries operations.

## Parameters / Member Variables
- `*data`: Array of SharedInvalidationMessage structures to be filled with retrieved messages
- `datasize`: Maximum number of messages that can be stored in the data array
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for SInvalReadLock synchronization)
  - SpinLockAcquire/SpinLockRelease (for msgnumLock protection)
  - [SharedInvalidationMessage](SharedInvalidationMessage.md) (message structure type)
  - [SISeg](SISeg.md) (shared invalidation segment structure)
  - [ProcState](../P/ProcState.md) (per-process state tracking)
  - MAXNUMMESSAGES (circular buffer size constant)
  - LW_SHARED (lock mode constant)
- Called from (representative examples):
  - ReceiveSharedInvalidMessages (sinval.c:99)
  - Cache invalidation processing routines

## Notes and Other Information
- Returns 0 if no messages available, positive count for retrieved messages, or -1 for reset condition
- Uses shared locking (SInvalReadLock) to allow concurrent readers while blocking writers
- Includes performance optimization with initial unlocked hasMessages check
- Implements circular buffer access using modulo arithmetic with MAXNUMMESSAGES
- Maintains backend-specific state including nextMsgNum, resetState, hasMessages, and signaled flags
- Part of PostgreSQL's distributed cache invalidation mechanism ensuring data consistency across processes
- Can safely run in parallel with other SIGetDataEntries instances and SIInsertDataEntries
- Handles message overflow conditions gracefully through the reset mechanism

## Simplified Source

```c
int SIGetDataEntries(SharedInvalidationMessage *data, int datasize)
{
    SISeg *segP = shmInvalBuffer;
    ProcState *stateP = &segP->procState[MyProcNumber];

    // Quick unlocked check - if no messages, return immediately
    if (!stateP->hasMessages)
        return 0;

    // Lock for safe message reading
    LWLockAcquire(SInvalReadLock, LW_SHARED);

    // Reset flag before reading to catch new messages
    stateP->hasMessages = false;

    // Get current maximum message number
    SpinLockAcquire(&segP->msgnumLock);
    int max = segP->maxMsgNum;
    SpinLockRelease(&segP->msgnumLock);

    // Handle reset condition
    if (stateP->resetState) {
        stateP->nextMsgNum = max;
        stateP->resetState = false;
        stateP->signaled = false;
        LWLockRelease(SInvalReadLock);
        return -1;  // Reset signal
    }

    // Copy available messages to data array
    int n = 0;
    while (n < datasize && stateP->nextMsgNum < max) {
        data[n++] = segP->buffer[stateP->nextMsgNum % MAXNUMMESSAGES];
        stateP->nextMsgNum++;
    }

    // Update state flags based on whether we caught up
    if (stateP->nextMsgNum >= max)
        stateP->signaled = false;        // Caught up completely
    else
        stateP->hasMessages = true;      // More messages remain

    LWLockRelease(SInvalReadLock);
    return n;  // Number of messages retrieved
}
```