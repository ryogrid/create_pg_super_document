# shm_mq_receive_bytes

## Location
[src/backend/storage/ipc/shm_mq.c:1079-1178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L1079-L1178)

## Overview
Waits for a minimum number of bytes to become available for reading from a shared message queue, handling buffer wrapping and various blocking scenarios.

## Definition
```c
static shm_mq_result shm_mq_receive_bytes(shm_mq_handle *mqh, Size bytes_needed, bool nowait, Size *nbytesp, void **datap)
```

## Detailed Description
This function implements the core waiting logic for reading data from shared message queues. It continuously polls the queue's atomic counters to determine data availability, handling three key scenarios: sufficient data is available, the buffer has wrapped around, or a wait is required. The function uses memory barriers to ensure proper ordering between reading metadata and actual data, and can operate in both blocking and non-blocking modes. When data becomes available, it returns a pointer to the readable data and the number of contiguous bytes available.

## Parameters / Member Variables
- `mqh`: Handle to the shared message queue being read from
- `bytes_needed`: Minimum number of bytes required to satisfy the read request
- `nowait`: If true, returns immediately with SHM_MQ_WOULD_BLOCK instead of waiting
- `nbytesp`: Output parameter set to the number of bytes available for reading
- `datap`: Output parameter set to pointer where data can be read from

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - pg_read_barrier
  - [shm_mq_inc_bytes_read](shm_mq_inc_bytes_read.md)
  - [WaitLatch](../W/WaitLatch.md)
  - [ResetLatch](../R/ResetLatch.md)
- Called from (representative examples):
  - [shm_mq_receive](shm_mq_receive.md)

## Notes and Other Information
- Uses atomic operations to read queue metadata without locks
- Implements proper memory barriers to prevent data races between metadata and payload reads
- Handles queue detachment gracefully by allowing completion of buffered messages
- Updates consume_pending counter to make buffer space available when waiting is required
- Uses latch-based waiting mechanism for efficient blocking when data is not immediately available