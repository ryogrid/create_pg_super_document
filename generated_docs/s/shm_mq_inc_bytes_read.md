# shm_mq_inc_bytes_read

## Location
[src/backend/storage/ipc/shm_mq.c:1270-1302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L1270-L1302)

## Overview
Atomically increments the count of bytes read from a shared message queue and signals the sender process that buffer space has become available.

## Definition
```c
static void shm_mq_inc_bytes_read(shm_mq *mq, Size n)
```

## Detailed Description
This function updates the queue's bytes_read counter to reflect data consumption by the receiver. It uses atomic operations to modify the counter and includes a read barrier to ensure proper memory ordering with respect to prior ring buffer reads. After updating the counter, it signals the sender process via its latch to notify that buffer space has become available for new writes. The function is designed to be called only by the receiver process, eliminating the need for more expensive atomic fetch-and-add operations.

## Parameters / Member Variables
- `mq`: Pointer to the shared message queue structure
- `n`: Number of bytes to increment the read counter by

## Dependencies
- Functions called/Symbols referenced:
  - pg_read_barrier
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - [SetLatch](../S/SetLatch.md)
- Called from (representative examples):
  - [shm_mq_receive](shm_mq_receive.md)
  - [shm_mq_receive_bytes](shm_mq_receive_bytes.md)

## Notes and Other Information
- Uses a read barrier to ensure proper ordering between ring buffer reads and counter updates
- Implements atomic counter increment using read-modify-write pattern instead of fetch-and-add for better performance
- Always signals the sender's latch to wake up blocked send operations when buffer space becomes available
- Assumes sender pointer is stable once initialized (no locking required for sender access)
- Critical for flow control mechanism that prevents sender from overwhelming the circular buffer

## Simplified Source

```c
// Simplified version of shm_mq_inc_bytes_read
static void shm_mq_inc_bytes_read(shm_mq *mq, Size n) {
    // Memory barrier to separate ring reads from counter update
    pg_read_barrier();

    // Atomically increment bytes read counter
    uint64 current_bytes = pg_atomic_read_u64(&mq->mq_bytes_read);
    pg_atomic_write_u64(&mq->mq_bytes_read, current_bytes + n);

    // Signal sender that buffer space is available
    PGPROC *sender = mq->mq_sender;
    Assert(sender != NULL);
    SetLatch(&sender->procLatch);
}
```

Key simplifications made:
- Added explanatory comments for memory barrier and atomic operations
- Preserved critical memory ordering and atomic counter update
- Maintained sender signaling mechanism for flow control
- Kept the assertion for sender validation