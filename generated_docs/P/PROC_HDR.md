# PROC_HDR

## Location
[src/include/storage/proc.h:370-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/proc.h#L370-L412)

## Overview
PROC_HDR is the central shared memory header structure that manages the global process array and maintains dense arrays of frequently-accessed PGPROC fields for optimal performance in PostgreSQL's multi-process architecture.

## Definition

```c
typedef struct PROC_HDR
{
	/* Array of PGPROC structures (not including dummies for prepared txns) */
	PGPROC	   *allProcs;

	/* Array mirroring PGPROC.xid for each PGPROC currently in the procarray */
	TransactionId *xids;

	/*
	 * Array mirroring PGPROC.subxidStatus for each PGPROC currently in the
	 * procarray.
	 */
	XidCacheStatus *subxidStates;

	/*
	 * Array mirroring PGPROC.statusFlags for each PGPROC currently in the
	 * procarray.
	 */
	uint8	   *statusFlags;

	/* Length of allProcs array */
	uint32		allProcCount;
	/* Head of list of free PGPROC structures */
	dlist_head	freeProcs;
	/* Head of list of autovacuum & special worker free PGPROC structures */
	dlist_head	autovacFreeProcs;
	/* Head of list of bgworker free PGPROC structures */
	dlist_head	bgworkerFreeProcs;
	/* Head of list of walsender free PGPROC structures */
	dlist_head	walsenderFreeProcs;
	/* First pgproc waiting for group XID clear */
	pg_atomic_uint32 procArrayGroupFirst;
	/* First pgproc waiting for group transaction status update */
	pg_atomic_uint32 clogGroupFirst;
	/* WALWriter process's latch */
	Latch	   *walwriterLatch;
	/* Checkpointer process's latch */
	Latch	   *checkpointerLatch;
	/* Current shared estimate of appropriate spins_per_delay value */
	int			spins_per_delay;
	/* Buffer id of the buffer that Startup process waits for pin on, or -1 */
	int			startupBufferPinWaitBufId;
} PROC_HDR;
```
## Detailed Description
PROC_HDR serves as the global control structure for PostgreSQL's process management system. There is one ProcGlobal structure (of type PROC_HDR) for the entire database cluster, managing all backend processes and maintaining critical performance optimizations.

The structure implements a dual-representation strategy: it maintains both the full PGPROC structures and separate dense arrays that mirror frequently-accessed fields. This design provides significant performance benefits:

1. **Dense Array Optimization**: Critical fields like transaction IDs (xids), subtransaction status (subxidStates), and process status flags (statusFlags) are stored in separate, tightly-packed arrays. This allows tight loops (like snapshot generation in GetSnapshotData()) to access many processes' data with optimal cache behavior.

2. **Cache Line Efficiency**: By separating frequently-changing data (xmin) from less frequently-changing data (xid, statusFlags), the design prevents unnecessary cache line invalidations across CPU cores and sockets.

3. **Reduced Indirection**: Dense arrays eliminate pointer chasing when scanning multiple processes, improving performance for operations that need to examine many/most backends.

4. **Dual Access Patterns**: Individual backends can efficiently access their own PGPROC fields without locking, while system-wide operations benefit from the dense arrays' performance characteristics.

The structure also manages process lifecycle through multiple free lists organized by process type (regular backends, autovacuum workers, background workers, WAL senders), enabling efficient allocation and reuse of PGPROC structures.

## Parameters / Member Variables
- `*allProcs`: Pointer to array of all PGPROC structures (excluding prepared transaction dummies)
- `*xids`: Dense array mirroring PGPROC.xid for processes currently in the procarray
- `*subxidStates`: Dense array mirroring PGPROC.subxidStatus for active processes
- `*statusFlags`: Dense array mirroring PGPROC.statusFlags for active processes
- `allProcCount`: Total number of PGPROC structures in the allProcs array
- `freeProcs`: Head of linked list containing unused PGPROC structures for regular backends
- `autovacFreeProcs`: Head of linked list for free autovacuum and special worker PGPROC structures
- `bgworkerFreeProcs`: Head of linked list for free background worker PGPROC structures
- `walsenderFreeProcs`: Head of linked list for free WAL sender PGPROC structures
- `procArrayGroupFirst`: Atomic pointer to first process waiting for group XID clearing
- `clogGroupFirst`: Atomic pointer to first process waiting for group transaction status update
- `*walwriterLatch`: Pointer to the WAL writer process's latch for signaling
- `*checkpointerLatch`: Pointer to the checkpointer process's latch for signaling
- `spins_per_delay`: Shared estimate of appropriate spin delay value for lock contention
- `startupBufferPinWaitBufId`: Buffer ID that startup process is waiting to pin, or -1 if none
## Dependencies
- Functions called/Symbols referenced:
  - [PGPROC](PGPROC.md) (main process structure type)
  - XidCacheStatus (subtransaction cache status enumeration)
  - [dlist_head](../d/dlist_head.md) (doubly-linked list head for free process lists)
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md) (atomic integer for group processing chains)
  - [Latch](../L/Latch.md) (process signaling mechanism)
- Called from (representative examples):
  - InitProcGlobal (global process system initialization)
  - ProcGlobalShmemSize (shared memory size calculation)
  - [ProcArrayGroupClearXid](ProcArrayGroupClearXid.md) (group transaction clearing)
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