# shm_mq_inc_bytes_written

## Location
src/backend/storage/ipc/shm_mq.c: 1303 - 1322

## Overview
Atomically increments the count of bytes written to a shared message queue with proper memory ordering to ensure data visibility to the receiver.

## Definition
```c
static void shm_mq_inc_bytes_written(shm_mq *mq, Size n)
```

## Detailed Description
This function updates the queue's bytes_written counter to reflect data production by the sender. It includes a write barrier before the counter update to ensure that all prior writes to the ring buffer are visible to other processes before the counter is incremented. This ordering guarantee is critical for correctness, as it prevents the receiver from reading the counter update before the actual data is visible. The function uses atomic operations but avoids more expensive fetch-and-add operations since only the sender modifies this counter.

## Parameters / Member Variables
- `mq`: Pointer to the shared message queue structure
- `n`: Number of bytes to increment the written counter by

## Dependencies
- Functions called/Symbols referenced:
  - pg_write_barrier
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
- Called from (representative examples):
  - [shm_mq_sendv](shm_mq_sendv.md)
  - [shm_mq_detach](shm_mq_detach.md)
  - [shm_mq_send_bytes](shm_mq_send_bytes.md)

## Notes and Other Information
- Uses a write barrier to ensure ring buffer writes complete before counter update becomes visible
- Critical memory ordering pairs with read barrier in shm_mq_receive_bytes for correct data synchronization
- Implements atomic counter increment using read-modify-write pattern instead of fetch-and-add for performance
- Only called by the sender process, eliminating need for multi-writer synchronization
- Essential for flow control mechanism that allows receiver to determine data availability