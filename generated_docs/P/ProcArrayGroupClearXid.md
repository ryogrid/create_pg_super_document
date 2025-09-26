# ProcArrayGroupClearXid

## Location
[src/backend/storage/ipc/procarray.c:792-906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L792-L906)

## Overview
ProcArrayGroupClearXid implements a group-based optimization for clearing transaction IDs during commit to reduce contention on ProcArrayLock when many processes are committing simultaneously.

## Definition

```c
static void
ProcArrayGroupClearXid(PGPROC *proc, TransactionId latestXid)
```
## Detailed Description
This function implements a lock batching mechanism to reduce contention around ProcArrayLock during transaction commits. When multiple processes are trying to commit at once, instead of each process acquiring the exclusive lock individually, processes add themselves to a list where the first process (leader) acquires the lock and performs the XID clearing operation for all processes in the group.

The function uses atomic operations to maintain a lock-free linked list of processes waiting for XID clearing. The leader process walks this list and calls ProcArrayEndTransactionInternal for each member, then wakes up all follower processes after releasing the lock.

## Parameters / Member Variables
- : Pointer to the PGPROC structure of the process whose XID needs to be cleared
- : The latest transaction ID to be recorded for maintaining completion tracking

## Dependencies
- Functions called/Symbols referenced:
  - GetNumberFromPGProc
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)
  - [pg_atomic_write_u32](../p/pg_atomic_write_u32.md)
  - [pg_atomic_compare_exchange_u32](../p/pg_atomic_compare_exchange_u32.md)
  - [pg_atomic_exchange_u32](../p/pg_atomic_exchange_u32.md)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
  - [PGSemaphoreLock](PGSemaphoreLock.md)
  - [PGSemaphoreUnlock](PGSemaphoreUnlock.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [ProcArrayEndTransactionInternal](ProcArrayEndTransactionInternal.md)
  - pg_write_barrier
- Called from (representative examples):
  - [ProcArrayEndTransaction](ProcArrayEndTransaction.md)

## Notes and Other Information
- Uses atomic operations to implement a lock-free queue for batching XID clearing operations
- The first process to join becomes the leader and handles all pending requests
- Follower processes wait on their semaphore until the leader completes their operation
- Memory barriers ensure proper ordering of operations across processes
- Significantly reduces lock contention in high-throughput transaction scenarios
- Part of PostgreSQL's performance optimization for concurrent transaction processing