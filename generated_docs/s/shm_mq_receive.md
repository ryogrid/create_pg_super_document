# shm_mq_receive

## Location
[src/backend/storage/ipc/shm_mq.c:572-819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L572-L819)

## Overview
Receives a complete message from a shared memory message queue, handling both contiguous and fragmented data efficiently.

## Definition
```c
shm_mq_result shm_mq_receive(shm_mq_handle *mqh, Size *nbytesp, void **datap, bool nowait)
```

## Detailed Description
This function implements the complete message reception protocol for shared memory queues. It reads length-prefixed messages and attempts to minimize data copying by returning direct pointers into shared memory when possible. For messages that wrap around the ring buffer or are received in fragments, it manages temporary buffers to reassemble complete messages.

Key operational features:
- **Zero-Copy Optimization**: When messages are available as contiguous blocks in shared memory, returns direct pointers without copying
- **Automatic Buffering**: For fragmented messages, transparently manages temporary buffers that grow as needed
- **Flow Control**: Implements receiver-side batching - only updates shared consumption counters when >1/4 of ring buffer is consumed
- **Robust State Management**: Handles partial reads across multiple calls, maintaining progress through complex state tracking
- **Sender Synchronization**: Waits for sender attachment and properly handles sender detachment scenarios

The function processes messages in phases: sender attachment verification, optional consumption updates, length word reading, and payload data reading with appropriate buffer management.

## Parameters / Member Variables
- `mqh`: Handle to the shared memory queue for receiving
- `nbytesp`: Output parameter set to the received message length in bytes
- `datap`: Output parameter set to point to message data (either direct shared memory or temporary buffer)
- `nowait`: If true, returns immediately when no data available instead of blocking

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_receive_bytes](shm_mq_receive_bytes.md) (low-level data reading)
  - [shm_mq_inc_bytes_read](shm_mq_inc_bytes_read.md) (update consumption counters)
  - [shm_mq_counterparty_gone](shm_mq_counterparty_gone.md) (sender status checking)
  - [shm_mq_wait_internal](shm_mq_wait_internal.md) (blocking wait implementation)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (buffer allocation)
  - pg_nextpower2_size_t (buffer size calculation)
- Called from (representative examples):
  - [HandleParallelMessages](../H/HandleParallelMessages.md) (parallel query message processing)
  - [TupleQueueReaderNext](../T/TupleQueueReaderNext.md) (tuple queue operations)
  - [LogicalParallelApplyLoop](../L/LogicalParallelApplyLoop.md) (logical replication)

## Notes and Other Information
- Only the designated receiver process (mq->mq_receiver == MyProc) can call this function
- Returned data pointers remain valid until the next receive operation on the same queue
- Prevents memory exhaustion by limiting message sizes to MaxAllocSize
- Uses efficient power-of-2 buffer growth strategy for temporary buffers up to MaxAllocSize
- Implements careful race condition handling for sender attachment detection in nowait mode
- Returns SHM_MQ_SUCCESS with data, SHM_MQ_WOULD_BLOCK if nowait and no data available, or SHM_MQ_DETACHED if sender disconnected
- Automatically manages alignment requirements and consumption accounting for optimal ring buffer utilization

## Simplified Source

```c
// Simplified version of shm_mq_receive
shm_mq_result shm_mq_receive(shm_mq_handle *mqh, Size *nbytesp, void **datap, bool nowait) {
    shm_mq *mq = mqh->mqh_queue;
    shm_mq_result res;
    Size rb = 0;
    Size nbytes;
    void *rawdata;

    Assert(mq->mq_receiver == MyProc);

    // Wait for sender to attach if not already done
    if (!mqh->mqh_counterparty_attached) {
        if (nowait) {
            // Check if sender is gone or never attached
            if (shm_mq_counterparty_gone(mq, mqh->mqh_handle) ||
                shm_mq_get_sender(mq) == NULL) {
                return is_gone ? SHM_MQ_DETACHED : SHM_MQ_WOULD_BLOCK;
            }
        } else {
            // Wait for sender attachment or return detached if failed
            if (!shm_mq_wait_internal(mq, &mq->mq_sender, mqh->mqh_handle)) {
                mq->mq_detached = true;
                return SHM_MQ_DETACHED;
            }
        }
        mqh->mqh_counterparty_attached = true;
    }

    // Update consumption counters if significant data consumed (>25% of ring)
    if (mqh->mqh_consume_pending > mq->mq_ring_size / 4) {
        shm_mq_inc_bytes_read(mq, mqh->mqh_consume_pending);
        mqh->mqh_consume_pending = 0;
    }

    // Read message length word (may require multiple attempts if fragmented)
    while (!mqh->mqh_length_word_complete) {
        res = shm_mq_receive_bytes(mqh, sizeof(Size) - mqh->mqh_partial_bytes,
                                   nowait, &rb, &rawdata);
        if (res != SHM_MQ_SUCCESS)
            return res;

        if (mqh->mqh_partial_bytes == 0 && rb >= sizeof(Size)) {
            // Got complete length word in one read
            nbytes = *(Size *) rawdata;
            Size needed = MAXALIGN(sizeof(Size)) + MAXALIGN(nbytes);

            // If entire message is available, return direct pointer
            if (rb >= needed) {
                mqh->mqh_consume_pending += needed;
                *nbytesp = nbytes;
                *datap = ((char *) rawdata) + MAXALIGN(sizeof(Size));
                return SHM_MQ_SUCCESS;
            }

            // Have length word but not full message
            mqh->mqh_expected_bytes = nbytes;
            mqh->mqh_length_word_complete = true;
            mqh->mqh_consume_pending += MAXALIGN(sizeof(Size));
            rb -= MAXALIGN(sizeof(Size));
        } else {
            // Length word is fragmented - copy to buffer and reassemble
            ensure_buffer_allocated(mqh, sizeof(Size));
            copy_partial_length_data(mqh, rawdata, rb);

            if (mqh->mqh_partial_bytes >= sizeof(Size)) {
                mqh->mqh_expected_bytes = *(Size *) mqh->mqh_buffer;
                mqh->mqh_length_word_complete = true;
                mqh->mqh_partial_bytes = 0;
            }
        }
    }

    nbytes = mqh->mqh_expected_bytes;

    // Validate message size
    if (nbytes > MaxAllocSize)
        ereport(ERROR, (errmsg("invalid message size %zu", nbytes)));

    if (mqh->mqh_partial_bytes == 0) {
        // Try to get entire message in one contiguous chunk
        res = shm_mq_receive_bytes(mqh, nbytes, nowait, &rb, &rawdata);
        if (res != SHM_MQ_SUCCESS)
            return res;

        if (rb >= nbytes) {
            // Got complete message - return direct pointer
            mqh->mqh_length_word_complete = false;
            mqh->mqh_consume_pending += MAXALIGN(nbytes);
            *nbytesp = nbytes;
            *datap = rawdata;
            return SHM_MQ_SUCCESS;
        }

        // Message wraps around buffer - ensure adequate buffer for copying
        ensure_buffer_size(mqh, nbytes);
    }

    // Copy message data in chunks until complete
    while (mqh->mqh_partial_bytes < nbytes) {
        if (rb > 0) {
            memcpy(&mqh->mqh_buffer[mqh->mqh_partial_bytes], rawdata, rb);
            mqh->mqh_partial_bytes += rb;
            mqh->mqh_consume_pending += MAXALIGN(rb);
        }

        if (mqh->mqh_partial_bytes >= nbytes)
            break;

        // Need more data
        Size still_needed = nbytes - mqh->mqh_partial_bytes;
        res = shm_mq_receive_bytes(mqh, still_needed, nowait, &rb, &rawdata);
        if (res != SHM_MQ_SUCCESS)
            return res;
        rb = Min(rb, still_needed);
    }

    // Return complete message and reset state
    *nbytesp = nbytes;
    *datap = mqh->mqh_buffer;
    mqh->mqh_length_word_complete = false;
    mqh->mqh_partial_bytes = 0;
    return SHM_MQ_SUCCESS;
}
```

Key simplifications made:
- Removed detailed race condition handling logic for clarity
- Abstracted complex buffer allocation into helper function concepts
- Simplified fragmented length word handling with conceptual helper functions
- Consolidated similar error checking and state management
- Reduced detailed memory alignment calculations to essential logic
- Maintained core algorithm: sender attachment → length reading → payload reading → return
- Preserved the zero-copy optimization path for contiguous messages