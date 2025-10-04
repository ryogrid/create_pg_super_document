# test_shm_mq_main

## Location
[src/test/modules/test_shm_mq/worker.c:47-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_shm_mq/worker.c#L47-L153)

## Overview
The main entry point function for a background worker process that demonstrates how to use shared memory message queues (shm_mq) for parallel computation in PostgreSQL.

## Definition
```c
void test_shm_mq_main(Datum main_arg)
```

## Detailed Description
This function serves as the background worker entrypoint for the test_shm_mq module, which demonstrates inter-process communication using PostgreSQL's shared memory message queue infrastructure. The function performs several key tasks:

1. **Signal Handling Setup**: Establishes SIGTERM handler using die() to ensure proper cleanup when the worker is terminated
2. **Dynamic Shared Memory Attachment**: Connects to a DSM segment passed via main_arg and locates the table of contents (TOC) structure
3. **Worker Registration**: Acquires a unique worker number from the shared header structure in a thread-safe manner
4. **Message Queue Attachment**: Sets up input and output message queue handles for communication
5. **Synchronization**: Signals the parent process that the worker is ready to begin processing
6. **Message Processing**: Performs the actual work of copying messages between queues
7. **Cleanup**: Detaches from the shared memory segment and exits

The function is designed as boilerplate code where only the attach_to_queues() and copy_messages() functions would typically be replaced for custom applications.

## Parameters / Member Variables
- `main_arg`: A Datum containing the DSM segment ID (as UInt32) that this worker should attach to for receiving instructions

## Dependencies
- Functions called/Symbols referenced:
  - [pqsignal](../p/pqsignal.md), BackgroundWorkerUnblockSignals (signal handling)
  - [dsm_attach](../d/dsm_attach.md), dsm_detach, dsm_segment_address (dynamic shared memory management)
  - [shm_toc_attach](../s/shm_toc_attach.md), shm_toc_lookup (shared memory table of contents)
  - [attach_to_queues](../a/attach_to_queues.md) (application-specific queue setup)
  - [copy_messages](../c/copy_messages.md) (application-specific message processing)
  - [BackendPidGetProc](../B/BackendPidGetProc.md), SetLatch (process communication)
  - [proc_exit](../p/proc_exit.md) (process termination)
- Called from (representative examples):
  - test_shm_mq_header (registered as background worker entry point)

## Notes and Other Information
- This function is part of PostgreSQL's test infrastructure for shared memory message queues
- The worker process has no ResourceOwner, so DSM mappings survive until process exit
- Uses spinlocks for thread-safe access to shared header data
- Implements a cooperative parallel processing pattern where multiple workers can process messages in a pipeline
- The PG_TEST_SHM_MQ_MAGIC constant is used to validate the shared memory segment integrity

## Simplified Source

```c
void
test_shm_mq_main(Datum main_arg)
{
    dsm_segment *seg;
    shm_toc *toc;
    shm_mq_handle *inqh;
    shm_mq_handle *outqh;
    volatile test_shm_mq_header *hdr;
    int myworkernumber;
    PGPROC *registrant;

    // Set up signal handling
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    // Attach to dynamic shared memory segment
    seg = dsm_attach(DatumGetUInt32(main_arg));
    if (seg == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("unable to map dynamic shared memory segment")));

    toc = shm_toc_attach(PG_TEST_SHM_MQ_MAGIC, dsm_segment_address(seg));
    if (toc == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("bad magic number in dynamic shared memory segment")));

    // Acquire worker number
    hdr = shm_toc_lookup(toc, 0, false);
    SpinLockAcquire(&hdr->mutex);
    myworkernumber = ++hdr->workers_attached;
    SpinLockRelease(&hdr->mutex);

    if (myworkernumber > hdr->workers_total)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("too many message queue testing workers already")));

    // Attach to message queues
    attach_to_queues(seg, toc, myworkernumber, &inqh, &outqh);

    // Signal that worker is ready
    SpinLockAcquire(&hdr->mutex);
    ++hdr->workers_ready;
    SpinLockRelease(&hdr->mutex);

    registrant = BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid);
    if (registrant == NULL) {
        elog(DEBUG1, "registrant backend has exited prematurely");
        proc_exit(1);
    }
    SetLatch(&registrant->procLatch);

    // Do the actual work
    copy_messages(inqh, outqh);

    // Clean up and exit
    dsm_detach(seg);
    proc_exit(1);
}
```