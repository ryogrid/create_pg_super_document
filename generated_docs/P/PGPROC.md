# PGPROC

## Location
src/include/storage/proc.h: 162 - 369

## Overview
PGPROC is a critical shared memory structure representing each backend process in PostgreSQL, containing all per-process state needed for transaction management, locking, and inter-process coordination.

## Definition


## Detailed Description
PGPROC is the fundamental per-process structure in PostgreSQL's shared memory architecture. Each backend process (including regular user backends, background workers, and auxiliary processes) has an associated PGPROC structure that contains all the state information necessary for that process to participate in PostgreSQL's transaction management, locking, and inter-process coordination systems.

The structure serves multiple critical roles:
1. **Transaction Management**: Stores the current transaction ID (xid) and transaction visibility information (xmin)
2. **Lock Management**: Tracks locks held and awaited by the process, including fast-path locks for optimization
3. **Process Coordination**: Provides mechanisms for inter-process signaling via semaphores and latches
4. **Subtransaction Caching**: Contains XidCache for efficient subtransaction visibility checking
5. **Group Processing**: Supports group-based optimizations for transaction commits and clog updates
6. **Wait Events**: Tracks what the process is currently waiting for (locks, I/O, etc.)

Some fields are mirrored in dense arrays within ProcGlobal for performance optimization, allowing tight loops to access frequently-used data without cache misses from the full PGPROC structure.

## Parameters / Member Variables
- : List linkage for when process is in various wait queues (MUST be first field)
- : Pointer to the global list that owns this PGPROC
- : Process semaphore for blocking/waking operations
- : Current wait status of the process
- : Generic latch for process signaling
- : Current top-level transaction ID (mirrored in dense arrays)
- : Minimum running XID for vacuum coordination
- : Backend process ID (0 for prepared transactions)
- : Offset into ProcGlobal dense arrays
- : Virtual transaction ID components (procNumber + lxid)
- : OID of database this backend is connected to
- : OID of role/user for this backend
- : OID of temporary schema for this backend
- : Flag indicating background worker vs regular backend
- : Hot standby conflict signal flag
- : LWLock wait state information
- : Mode of LWLock being waited for
- : Position in LWLock wait queue
- : Position in condition variable wait queue
- : Lock object currently being waited for
- : Per-holder information for awaited lock
- : Type of lock mode being requested
- : Bitmask of lock types already held
- : Timestamp when lock wait began
- : Checkpoint delay flags (DELAY_CHKPT_*)
- : Process status flags (mirrored in dense arrays)
- : LSN being waited for in synchronous replication
- : Synchronous replication wait state
- : Linkage in synchronous replication queue
- : Per-partition lists of locks held by this process
- : Status of subtransaction cache
- : Cache of subtransaction XIDs (XidCache structure)
- : Flag for group XID clearing participation
- : Next member in group XID clearing chain
- : Transaction ID for group processing
- : Current wait event information for monitoring
- : Flag for group clog update participation
- : Next member in clog group update chain
- : Transaction ID for clog group update
- : Transaction status for clog group update
- : Clog page for group update
- : WAL LSN for group commit record
- : LWLock protecting fast-path lock state
- : Bitmask of fast-path lock modes held
- : Relation OIDs for fast-path lock slots
- : Flag indicating fast-path virtual XID lock held
- : Local transaction ID for fast-path VXID lock
- : Pointer to lock group leader (if member)
- : List of lock group members (if leader)
- : Linkage as member of a lock group

## Dependencies
- Functions called/Symbols referenced:
  - ProcNumber (for virtual transaction ID)
  - LocalTransactionId (for virtual transaction ID)
  - XidCache (for subtransaction caching)
  - Various lock and latch types (LOCK, PROCLOCK, LWLock, etc.)
- Called from (representative examples):
  - InitProcess (process initialization)
  - ProcArrayAdd (adding to process array)
  - ProcSleep (lock waiting)
  - GetSnapshotData (transaction visibility)
  - DeadLockCheck (deadlock detection)

## Notes and Other Information
- The  field MUST be the first field in the structure for proper operation of ProcSleep/ProcWakeup functions
- Many fields are mirrored in dense arrays within ProcGlobal for performance optimization
- Access to mirrored fields requires holding ProcArrayLock or XidGenLock to prevent race conditions
- Prepared transactions have special PGPROC entries with pid=0
- The structure supports both regular backends and various types of background worker processes
- Fast-path locking provides optimization for frequently-accessed relation locks
- Group processing mechanisms (group XID clearing, group clog updates) reduce contention for high-throughput workloads
- Lock groups allow parallel workers to share lock state for improved concurrency