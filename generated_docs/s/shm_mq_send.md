# shm_mq_send

## Location
src/backend/storage/ipc/shm_mq.c: 329 - 360

## Overview
Writes a single contiguous message into a shared memory message queue.

## Definition
```c
shm_mq_result shm_mq_send(shm_mq_handle *mqh, Size nbytes, const void *data, bool nowait, bool force_flush)
```

## Detailed Description
This function provides a convenient interface for sending a single contiguous block of data through a shared memory message queue. It acts as a wrapper around the more general `shm_mq_sendv` function by constructing a single I/O vector element and delegating the actual sending operation.

The function handles the common case where the message data is stored in a single contiguous memory buffer, eliminating the need for callers to manually construct I/O vector arrays for simple sends.

## Parameters / Member Variables
- `mqh`: Handle to the shared memory queue for sending
- `nbytes`: Size of the data to send in bytes
- `data`: Pointer to the contiguous data buffer to send
- `nowait`: If true, the function returns immediately if the queue is full rather than waiting
- `force_flush`: If true, forces immediate flushing of any pending data to ensure message delivery

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_sendv](shm_mq_sendv.md) (vectorized send implementation)
  - shm_mq_iovec (I/O vector structure)
- Called from (representative examples):
  - [tqueueReceiveSlot](../t/tqueueReceiveSlot.md) (tuple queue operations)
  - [test_shm_mq](../t/test_shm_mq.md) (testing infrastructure)
  - [copy_messages](../c/copy_messages.md) (test worker processes)

## Notes and Other Information
- This is a convenience wrapper that simplifies the most common sending scenario
- The actual implementation is handled by shm_mq_sendv with a single I/O vector element
- Returns shm_mq_result indicating success, queue full, or detached state
- For sending multiple non-contiguous buffers in a single message, use shm_mq_sendv directly
- The force_flush parameter is useful when immediate delivery is critical for correctness