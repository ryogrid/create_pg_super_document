# PROC_HDR

## Location
src/include/storage/proc.h: 370 - 412

## Overview
PROC_HDR is the central shared memory header structure that manages the global process array and maintains dense arrays of frequently-accessed PGPROC fields for optimal performance in PostgreSQL's multi-process architecture.

## Definition


## Detailed Description
PROC_HDR serves as the global control structure for PostgreSQL's process management system. There is one ProcGlobal structure (of type PROC_HDR) for the entire database cluster, managing all backend processes and maintaining critical performance optimizations.

The structure implements a dual-representation strategy: it maintains both the full PGPROC structures and separate dense arrays that mirror frequently-accessed fields. This design provides significant performance benefits:

1. **Dense Array Optimization**: Critical fields like transaction IDs (xids), subtransaction status (subxidStates), and process status flags (statusFlags) are stored in separate, tightly-packed arrays. This allows tight loops (like snapshot generation in GetSnapshotData()) to access many processes' data with optimal cache behavior.

2. **Cache Line Efficiency**: By separating frequently-changing data (xmin) from less frequently-changing data (xid, statusFlags), the design prevents unnecessary cache line invalidations across CPU cores and sockets.

3. **Reduced Indirection**: Dense arrays eliminate pointer chasing when scanning multiple processes, improving performance for operations that need to examine many/most backends.

4. **Dual Access Patterns**: Individual backends can efficiently access their own PGPROC fields without locking, while system-wide operations benefit from the dense arrays' performance characteristics.

The structure also manages process lifecycle through multiple free lists organized by process type (regular backends, autovacuum workers, background workers, WAL senders), enabling efficient allocation and reuse of PGPROC structures.

## Parameters / Member Variables
- : Pointer to array of all PGPROC structures (excluding prepared transaction dummies)
- : Dense array mirroring PGPROC.xid for processes currently in the procarray
- : Dense array mirroring PGPROC.subxidStatus for active processes
- : Dense array mirroring PGPROC.statusFlags for active processes
- : Total number of PGPROC structures in the allProcs array
- : Head of linked list containing unused PGPROC structures for regular backends
- : Head of linked list for free autovacuum and special worker PGPROC structures
- : Head of linked list for free background worker PGPROC structures
- : Head of linked list for free WAL sender PGPROC structures
- : Atomic pointer to first process waiting for group XID clearing
- : Atomic pointer to first process waiting for group transaction status update
- : Pointer to the WAL writer process's latch for signaling
- : Pointer to the checkpointer process's latch for signaling
- : Shared estimate of appropriate spin delay value for lock contention
- : Buffer ID that startup process is waiting to pin, or -1 if none

## Dependencies
- Functions called/Symbols referenced:
  - PGPROC (main process structure type)
  - XidCacheStatus (subtransaction cache status enumeration)
  - dlist_head (doubly-linked list head for free process lists)
  - pg_atomic_uint32 (atomic integer for group processing chains)
  - Latch (process signaling mechanism)
- Called from (representative examples):
  - InitProcGlobal (global process system initialization)
  - ProcGlobalShmemSize (shared memory size calculation)
  - ProcArrayGroupClearXid (group transaction clearing)
  - TransactionGroupUpdateXidStatus (group transaction status updates)

## Notes and Other Information
- Dense array access requires holding either ProcArrayLock or XidGenLock to prevent race conditions with pgxactoff changes
- The dense arrays only contain entries for processes added to the shared array via ProcArrayAdd(), unlike allProcs which may have unused entries interspersed
- Mirrored values in both PGPROC and dense arrays must be maintained coherently
- The dual-representation design optimizes for two access patterns: single-backend checks (use PGPROC fields) and multi-backend scans (use dense arrays)
- Free process lists are segregated by process type to enable specialized allocation policies
- Group processing features (procArrayGroupFirst, clogGroupFirst) provide optimizations for high-throughput transaction processing
- The structure supports efficient process lifecycle management from allocation through deallocation
- Critical for PostgreSQL's MVCC (Multi-Version Concurrency Control) implementation through transaction visibility management