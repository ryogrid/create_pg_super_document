# SIInsertDataEntries

## Location
[src/backend/storage/ipc/sinvaladt.c:370-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/sinvaladt.c#L370-L472)

## Overview
SIInsertDataEntries adds new invalidation messages to the shared invalidation buffer, managing buffer space and notifying all active backend processes about the new messages.

## Definition
void SIInsertDataEntries(const SharedInvalidationMessage *data, int n)

## Detailed Description
This function inserts cache invalidation messages into the circular shared buffer and ensures all active backend processes are notified. The function implements several important mechanisms:

1. **Batching**: Processes messages in batches of WRITE_QUANTUM size to avoid holding locks too long
2. **Buffer Management**: Monitors buffer fullness and calls SICleanupQueue when necessary to free space
3. **Circular Buffer Insertion**: Adds messages to the circular buffer using modulo arithmetic
4. **Atomic Updates**: Uses spinlocks to atomically update maxMsgNum
5. **Process Notification**: Sets hasMessages flag for all active processes to ensure they process the new invalidations

The function handles arbitrarily large numbers of messages by breaking them into manageable chunks, ensuring system responsiveness while maintaining data consistency.

## Parameters / Member Variables
- : Pointer to an array of SharedInvalidationMessage structures containing the invalidation data to insert
- : Number of invalidation messages in the data array

## Dependencies
- Functions called/Symbols referenced:
  - Min (macro)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [SICleanupQueue](SICleanupQueue.md)
  - SpinLockAcquire
  - SpinLockRelease
- Data types referenced:
  - [SharedInvalidationMessage](SharedInvalidationMessage.md)
  - [SISeg](SISeg.md)
  - [ProcState](../P/ProcState.md)
- Constants referenced:
  - WRITE_QUANTUM
  - MAXNUMMESSAGES
  - LW_EXCLUSIVE
- [Variables](../V/Variables.md) referenced:
  - shmInvalBuffer
  - SInvalWriteLock
- Called from (representative examples):
  - [SendSharedInvalidMessages](SendSharedInvalidMessages.md)

## Notes and Other Information
- The function is designed to handle large batches of invalidation messages efficiently without monopolizing system resources
- Buffer cleanup is performed proactively based on fullness thresholds and reactively when the buffer is full
- The circular buffer design allows for efficient memory usage with a fixed-size buffer
- Memory barriers are crucial for ensuring that maxMsgNum updates are visible before process notification
- The hasMessages flag serves as an optimization to avoid unnecessary processing by backends with no new messages
- Lock ordering is important: SInvalWriteLock is acquired first, then individual spinlocks for atomic operations
- The function must handle the case where buffer cleanup might not free enough space, requiring multiple cleanup attempts
- Process notification occurs after all messages are inserted to minimize the window where processes might see incomplete message sets

## Simplified Source

```c
void SIInsertDataEntries(const SharedInvalidationMessage *data, int n)
{
    SISeg *segP = shmInvalBuffer;

    // Process messages in batches to avoid holding locks too long
    while (n > 0)
    {
        int nthistime = Min(n, WRITE_QUANTUM);
        int numMsgs;
        int max;
        int i;

        n -= nthistime;

        LWLockAcquire(SInvalWriteLock, LW_EXCLUSIVE);

        // Clean queue if buffer is full or exceeds threshold
        for (;;)
        {
            numMsgs = segP->maxMsgNum - segP->minMsgNum;
            if (numMsgs + nthistime > MAXNUMMESSAGES ||
                numMsgs >= segP->nextThreshold)
                SICleanupQueue(true, nthistime);
            else
                break;
        }

        // Insert new messages into circular buffer
        max = segP->maxMsgNum;
        while (nthistime-- > 0)
        {
            segP->buffer[max % MAXNUMMESSAGES] = *data++;
            max++;
        }

        // Update maxMsgNum atomically using spinlock
        SpinLockAcquire(&segP->msgnumLock);
        segP->maxMsgNum = max;
        SpinLockRelease(&segP->msgnumLock);

        // Notify all processes about new messages
        for (i = 0; i < segP->numProcs; i++)
        {
            ProcState *stateP = &segP->procState[segP->pgprocnos[i]];
            stateP->hasMessages = true;
        }

        LWLockRelease(SInvalWriteLock);
    }
}
```