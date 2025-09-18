# LWLock

## Location
[src/include/storage/lwlock.h:41-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lwlock.h#L41-L50)

## Overview
LWLock (Lightweight Lock) is the fundamental locking structure in PostgreSQL used for protecting shared memory resources with minimal overhead and high performance.

## Definition


## Detailed Description
LWLock is PostgreSQL's lightweight locking mechanism designed for high-performance synchronization of shared memory access. Unlike heavier database locks, LWLocks are optimized for short-duration operations and provide both shared (read) and exclusive (write) locking modes. The structure uses atomic operations for the state field to minimize contention and improve scalability in multi-processor environments.

The lock state is managed through atomic operations on the state field, which tracks both the number of shared lockers and whether an exclusive lock is held. When processes cannot immediately acquire a lock, they are queued in the waiters list as PGPROC entries.

## Parameters / Member Variables
- : Identifies the lock tranche (group) for statistics and debugging purposes
- : Atomic variable storing lock state including shared lock count and exclusive lock flag
- : Queue of processes (PGPROC) waiting to acquire this lock
- : (Debug only) Atomic counter of waiting processes for debugging
- : (Debug only) Pointer to the last process that held an exclusive lock

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md) (atomic operations)
  - proclist_head (process list management)
  - [PGPROC](../P/PGPROC.md) (process control block)
- Called from (representative examples):
  - [BufferDesc](../B/BufferDesc.md) (buffer management)
  - [ReplicationSlot](../R/ReplicationSlot.md) (replication slot management)
  - SlruCtl (SLRU cache control)
  - [BufTableHashPartition](../B/BufTableHashPartition.md) (buffer hash table partitions)

## Notes and Other Information
- LWLocks are designed to be embedded in other structures rather than allocated separately
- The structure should not be manipulated directly outside of lwlock.c
- Debug fields are only available when LOCK_DEBUG is defined during compilation
- LWLocks support both shared (multiple readers) and exclusive (single writer) access modes
- The atomic state field enables lock-free fast paths for common operations
- LWLocks are used extensively throughout PostgreSQL for protecting shared data structures like buffer pools, hash tables, and various control structures