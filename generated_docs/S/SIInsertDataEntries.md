# SIInsertDataEntries

## Location
src/backend/storage/ipc/sinvaladt.c: 370 - 472

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
  - LWLockAcquire
  - LWLockRelease
  - SICleanupQueue
  - SpinLockAcquire
  - SpinLockRelease
- Data types referenced:
  - SharedInvalidationMessage
  - SISeg
  - ProcState
- Constants referenced:
  - WRITE_QUANTUM
  - MAXNUMMESSAGES
  - LW_EXCLUSIVE
- Variables referenced:
  - shmInvalBuffer
  - SInvalWriteLock
- Called from (representative examples):
  - SendSharedInvalidMessages

## Notes and Other Information
- The function is designed to handle large batches of invalidation messages efficiently without monopolizing system resources
- Buffer cleanup is performed proactively based on fullness thresholds and reactively when the buffer is full
- The circular buffer design allows for efficient memory usage with a fixed-size buffer
- Memory barriers are crucial for ensuring that maxMsgNum updates are visible before process notification
- The hasMessages flag serves as an optimization to avoid unnecessary processing by backends with no new messages
- Lock ordering is important: SInvalWriteLock is acquired first, then individual spinlocks for atomic operations
- The function must handle the case where buffer cleanup might not free enough space, requiring multiple cleanup attempts
- Process notification occurs after all messages are inserted to minimize the window where processes might see incomplete message sets