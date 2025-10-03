# shm_mq_create

## Location
[src/backend/storage/ipc/shm_mq.c:177-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L177-L205)

## Overview
Initializes a new shared message queue structure in shared memory, setting up the basic queue parameters and atomic counters for inter-process communication.

## Definition
shm_mq *shm_mq_create(void *address, Size size)

## Detailed Description
The shm_mq_create function initializes a shared memory message queue at the provided memory address. It sets up the queue header with proper alignment, initializes synchronization primitives (mutex and atomic counters), and calculates the usable ring buffer size. The function ensures that the provided memory region is properly aligned and large enough to accommodate the queue structure plus data storage. The initialized queue is ready for sender and receiver processes to be attached via shm_mq_set_sender and shm_mq_set_receiver.

## Parameters / Member Variables
- address: Pointer to the shared memory location where the queue will be created
- size: Total size of the shared memory region allocated for the queue (including header and data ring)

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN_DOWN
  - SpinLockInit  
  - [pg_atomic_init_u64](../p/pg_atomic_init_u64.md)
  - [shm_mq](shm_mq.md) (structure)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)
  - [ReinitializeParallelDSM](../R/ReinitializeParallelDSM.md)
  - [ExecParallelSetupTupleQueues](../E/ExecParallelSetupTupleQueues.md)
  - [pa_setup_dsm](../p/pa_setup_dsm.md)
  - [setup_dynamic_shared_memory](setup_dynamic_shared_memory.md)

## Notes and Other Information
- The function performs MAXALIGN_DOWN on the size to ensure proper memory alignment
- Initializes atomic counters for bytes_read and bytes_written to 0
- Sets both sender and receiver to NULL initially - they must be set separately
- The ring buffer size is calculated by subtracting the header size from total size
- Located in src/backend/storage/ipc/shm_mq.c:177-205

## Simplified Source

```c
shm_mq *shm_mq_create(void *address, Size size) {
    shm_mq *mq = address;

    // Calculate data area offset with proper alignment
    Size data_offset = MAXALIGN(offsetof(shm_mq, mq_ring));
    size = MAXALIGN_DOWN(size);

    // Initialize queue header and synchronization
    SpinLockInit(&mq->mq_mutex);
    mq->mq_receiver = NULL;
    mq->mq_sender = NULL;

    // Initialize atomic byte counters
    pg_atomic_init_u64(&mq->mq_bytes_read, 0);
    pg_atomic_init_u64(&mq->mq_bytes_written, 0);

    // Set ring buffer size and offset
    mq->mq_ring_size = size - data_offset;
    mq->mq_detached = false;
    mq->mq_ring_offset = data_offset - offsetof(shm_mq, mq_ring);

    return mq;
}
```