# ReplicationSlot

## Location
[src/include/replication/slot.h:148-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/slot.h#L148-L211)

## Overview
ReplicationSlot represents the shared memory state of a single replication slot, managing both in-memory runtime state and persistent data with a sophisticated locking model to ensure safe concurrent access across multiple backend processes.

## Definition
```c
typedef struct ReplicationSlot
{
    /* lock, on same cacheline as effective_xmin */
    slock_t     mutex;

    /* is this slot defined */
    bool        in_use;

    /* Who is streaming out changes for this slot? 0 in unused slots. */
    pid_t       active_pid;

    /* any outstanding modifications? */
    bool        just_dirtied;
    bool        dirty;

    /*
     * For logical decoding, it's extremely important that we never remove any
     * data that's still needed for decoding purposes, even after a crash;
     * otherwise, decoding will produce wrong answers.  Ordinary streaming
     * replication also needs to prevent old row versions from being removed
     * too soon, but the worst consequence we might encounter there is
     * unwanted query cancellations on the standby.  Thus, for logical
     * decoding, this value represents the latest xmin that has actually been
     * written to disk, whereas for streaming replication, it's just the same
     * as the persistent value (data.xmin).
     */
    TransactionId effective_xmin;
    TransactionId effective_catalog_xmin;

    /* data surviving shutdowns and crashes */
    ReplicationSlotPersistentData data;

    /* is somebody performing io on this slot? */
    LWLock      io_in_progress_lock;

    /* Condition variable signaled when active_pid changes */
    ConditionVariable active_cv;

    /* all the remaining data is only used for logical slots */

    /*
     * When the client has confirmed flushes >= candidate_xmin_lsn we can
     * advance the catalog xmin.  When restart_valid has been passed,
     * restart_lsn can be increased.
     */
    TransactionId candidate_catalog_xmin;
    XLogRecPtr  candidate_xmin_lsn;
    XLogRecPtr  candidate_restart_valid;
    XLogRecPtr  candidate_restart_lsn;

    /*
     * This value tracks the last confirmed_flush LSN flushed which is used
     * during a shutdown checkpoint to decide if logical's slot data should be
     * forcibly flushed or not.
     */
    XLogRecPtr  last_saved_confirmed_flush;

    /*
     * The time when the slot became inactive. For synced slots on a standby
     * server, it represents the time when slot synchronization was most
     * recently stopped.
     */
    TimestampTz inactive_since;
} ReplicationSlot;
```

## Detailed Description
This structure implements the complete in-memory representation of a replication slot with a sophisticated concurrency control model. It uses a two-level locking approach: ReplicationSlotControlLock for slot lifecycle operations and per-slot mutex for field-level protection. The structure maintains both runtime state (like active processes and dirty flags) and cached persistent data, with special handling for effective xmin values that ensure data consistency for logical decoding even across crashes.

## Parameters / Member Variables
- `mutex`: Spinlock protecting individual fields, used by slot-owning backend for updates
- `in_use`: Boolean flag indicating if this slot is currently defined and active
- `active_pid`: Process ID of the backend currently streaming from this slot (0 if unused)
- `just_dirtied`: Flag indicating recent modifications that need immediate attention
- `dirty`: Flag indicating the slot has modifications that need to be persisted
- `effective_xmin`: Transaction horizon actually written to disk (critical for logical decoding)
- `effective_catalog_xmin`: Catalog-specific transaction horizon written to disk
- `data`: Embedded persistent data structure containing all crash-surviving information
- `io_in_progress_lock`: LWLock protecting I/O operations on this slot
- `active_cv`: Condition variable for signaling changes in active_pid status
- `candidate_catalog_xmin`: Proposed new catalog xmin waiting for client confirmation
- `candidate_xmin_lsn`: LSN threshold for advancing catalog xmin
- `candidate_restart_valid`: LSN indicating when restart_lsn advancement is valid
- `candidate_restart_lsn`: Proposed new restart_lsn waiting for validation
- `last_saved_confirmed_flush`: Tracks last confirmed_flush LSN written during checkpoints
- `inactive_since`: Timestamp marking when the slot became inactive or sync stopped

## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](../s/slock_t.md) (for mutex synchronization)
  - pid_t (for process tracking)
  - [ReplicationSlotPersistentData](ReplicationSlotPersistentData.md) (for persistent state)
  - [LWLock](../L/LWLock.md) (for I/O coordination)
  - ConditionVariable (for process signaling)
- Called from (representative examples):
  - [CreateDecodingContext](../C/CreateDecodingContext.md)
  - [ReplicationSlotCreate](ReplicationSlotCreate.md)
  - [ReplicationSlotAcquire](ReplicationSlotAcquire.md)
  - [ReplicationSlotRelease](ReplicationSlotRelease.md)
  - [LogicalIncreaseXminForSlot](../L/LogicalIncreaseXminForSlot.md)

## Notes and Other Information
The structure implements a complex locking model where the ReplicationSlotControlLock controls slot lifecycle (in_use flag) while individual field access is protected by the per-slot mutex. Only the backend owning a slot can update its fields without taking the mutex for reads, but concurrent backends must hold the mutex for all access. The effective_xmin values are critical for logical decoding correctness and represent the most recently disk-flushed transaction horizons, ensuring that required data is never prematurely removed even after crashes. Logical-specific fields (candidate_* and last_saved_confirmed_flush) are only meaningful for logical replication slots.