# pa_setup_dsm

## Location
[src/backend/replication/logical/applyparallelworker.c:327-403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L327-L403)

## Overview
Sets up a dynamic shared memory segment for communication between leader and parallel apply workers in PostgreSQL logical replication.

## Definition

```c
static bool
pa_setup_dsm(ParallelApplyWorkerInfo *winfo)
```
## Detailed Description
This function creates and configures a dynamic shared memory (DSM) segment that facilitates communication between the leader apply worker and parallel apply workers. The segment contains a control region with worker information, a message queue for sending data to the worker, and an error queue for receiving error messages from the worker. The function uses the shared memory table of contents (TOC) mechanism to organize the different components within the segment.

## Parameters / Member Variables
- `*winfo`: Pointer to ParallelApplyWorkerInfo structure that will be populated with DSM handles and shared memory references
## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_initialize_estimator
  - shm_toc_estimate_chunk
  - shm_toc_estimate_keys
  - [shm_toc_estimate](../s/shm_toc_estimate.md)
  - [dsm_create](../d/dsm_create.md)
  - [shm_toc_create](../s/shm_toc_create.md)
  - [shm_toc_allocate](../s/shm_toc_allocate.md)
  - [shm_toc_insert](../s/shm_toc_insert.md)
  - [shm_mq_create](../s/shm_mq_create.md)
  - [shm_mq_set_sender](../s/shm_mq_set_sender.md)
  - [shm_mq_set_receiver](../s/shm_mq_set_receiver.md)
  - [shm_mq_attach](../s/shm_mq_attach.md)
  - SpinLockInit
  - [pg_atomic_init_u32](pg_atomic_init_u32.md)
- Called from:
  - [pa_launch_parallel_worker](pa_launch_parallel_worker.md)

## Notes and Other Information
- Creates three main components in shared memory: ParallelApplyWorkerShared header, message queue, and error queue
- Uses DSM_QUEUE_SIZE and DSM_ERROR_QUEUE_SIZE constants for queue sizing
- Initializes shared state with PARALLEL_TRANS_UNKNOWN transaction state and FS_EMPTY fileset state
- Sets up bidirectional communication: leader sends to worker via message queue, worker sends errors back via error queue
- Returns false on failure (e.g., unable to create DSM segment), true on success
- Part of PostgreSQL's logical replication parallel processing system located in src/backend/replication/logical/applyparallelworker.c:327-403

## Simplified Source

```c
static bool
pa_setup_dsm(ParallelApplyWorkerInfo *winfo)
{
    shm_toc_estimator e;
    Size segsize;
    dsm_segment *seg;
    shm_toc *toc;
    ParallelApplyWorkerShared *shared;
    shm_mq *mq;
    Size queue_size = DSM_QUEUE_SIZE;
    Size error_queue_size = DSM_ERROR_QUEUE_SIZE;

    // Estimate shared memory requirements for all components
    shm_toc_initialize_estimator(&e);
    shm_toc_estimate_chunk(&e, sizeof(ParallelApplyWorkerShared));
    shm_toc_estimate_chunk(&e, queue_size);
    shm_toc_estimate_chunk(&e, error_queue_size);
    shm_toc_estimate_keys(&e, 3);
    segsize = shm_toc_estimate(&e);

    // Create shared memory segment and table of contents
    seg = dsm_create(shm_toc_estimate(&e), 0);
    if (!seg)
        return false;

    toc = shm_toc_create(PG_LOGICAL_APPLY_SHM_MAGIC, dsm_segment_address(seg), segsize);

    // Initialize shared worker control structure
    shared = shm_toc_allocate(toc, sizeof(ParallelApplyWorkerShared));
    SpinLockInit(&shared->mutex);
    shared->xact_state = PARALLEL_TRANS_UNKNOWN;
    pg_atomic_init_u32(&(shared->pending_stream_count), 0);
    shared->last_commit_end = InvalidXLogRecPtr;
    shared->fileset_state = FS_EMPTY;
    shm_toc_insert(toc, PARALLEL_APPLY_KEY_SHARED, shared);

    // Create message queue for leader -> worker communication
    mq = shm_mq_create(shm_toc_allocate(toc, queue_size), queue_size);
    shm_toc_insert(toc, PARALLEL_APPLY_KEY_MQ, mq);
    shm_mq_set_sender(mq, MyProc);
    winfo->mq_handle = shm_mq_attach(mq, seg, NULL);

    // Create error queue for worker -> leader communication
    mq = shm_mq_create(shm_toc_allocate(toc, error_queue_size), error_queue_size);
    shm_toc_insert(toc, PARALLEL_APPLY_KEY_ERROR_QUEUE, mq);
    shm_mq_set_receiver(mq, MyProc);
    winfo->error_mq_handle = shm_mq_attach(mq, seg, NULL);

    // Store results in worker info
    winfo->dsm_seg = seg;
    winfo->shared = shared;

    return true;
}
```