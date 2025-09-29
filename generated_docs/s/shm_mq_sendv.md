# shm_mq_sendv

## Location
[src/backend/storage/ipc/shm_mq.c:361-571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L361-L571)

## Overview
Writes a message into a shared memory queue from multiple non-contiguous memory buffers using vectorized I/O.

## Definition
```c
shm_mq_result shm_mq_sendv(shm_mq_handle *mqh, shm_mq_iovec *iov, int iovcnt, bool nowait, bool force_flush)
```

## Detailed Description
This function provides the core implementation for sending messages through shared memory queues, supporting both simple contiguous data (via shm_mq_send wrapper) and complex vectorized writes from multiple buffers. It handles the complete message transmission protocol including length prefixing, data alignment requirements, flow control, and receiver notification.

Key operational aspects:
- **Message Format**: Each message is prefixed with a Size-typed length word followed by the actual data
- **Alignment Handling**: All data chunks except the last must be MAXALIGN-ed, requiring careful buffer management for non-aligned I/O vectors
- **Flow Control**: Supports both blocking (default) and non-blocking modes for handling full queues
- **Partial Transmission**: Maintains state to handle interrupted sends that can be resumed
- **Batching**: Optimizes notifications by batching writes and only notifying when significant data accumulates or force_flush is requested

## Parameters / Member Variables
- `mqh`: Handle to the shared memory queue for sending
- `iov`: Array of I/O vector elements containing data pointers and lengths
- `iovcnt`: Number of elements in the I/O vector array
- `nowait`: If true, returns immediately when queue is full instead of blocking
- `force_flush`: If true, immediately updates written byte count and notifies receiver

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_send_bytes](shm_mq_send_bytes.md) (low-level byte writing)
  - [shm_mq_inc_bytes_written](shm_mq_inc_bytes_written.md) (update shared counters)
  - [SetLatch](../S/SetLatch.md) (receiver notification)
  - MAXALIGN_DOWN (alignment calculations)
- Called from (representative examples):
  - [shm_mq_send](shm_mq_send.md) (simple send wrapper)
  - [mq_putmessage](../m/mq_putmessage.md) (libpq message passing)

## Notes and Other Information
- Only the designated sender process (mq->mq_sender == MyProc) can call this function
- Prevents overwhelming receivers by limiting message size to MaxAllocSize
- Handles partial writes gracefully - interrupted sends can be resumed with identical parameters
- Uses efficient batching strategy: notifications occur when >1/4 of ring buffer is written or force_flush is true
- Maintains alignment requirements by copying small unaligned chunks to temporary aligned buffers
- Automatically detects when receiver attaches and caches this state for performance
- Returns SHM_MQ_SUCCESS on completion, SHM_MQ_WOULD_BLOCK if nowait and queue full, or SHM_MQ_DETACHED if queue disconnected

## Simplified Source

```c
shm_mq_result shm_mq_sendv(shm_mq_handle *mqh, shm_mq_iovec *iov, int iovcnt,
                           bool nowait, bool force_flush)
{
    shm_mq *mq = mqh->mqh_queue;
    Size nbytes = 0;
    int which_iov = 0;
    Size offset;

    Assert(mq->mq_sender == MyProc);

    // Calculate total message size
    for (int i = 0; i < iovcnt; ++i)
        nbytes += iov[i].len;

    // Prevent overwhelming the receiver
    if (nbytes > MaxAllocSize)
        ereport(ERROR, (errmsg("message too large: %zu bytes", nbytes)));

    // First, send the length word
    while (!mqh->mqh_length_word_complete)
    {
        Size bytes_written;
        shm_mq_result res = shm_mq_send_bytes(mqh,
                                             sizeof(Size) - mqh->mqh_partial_bytes,
                                             ((char *) &nbytes) + mqh->mqh_partial_bytes,
                                             nowait, &bytes_written);

        if (res == SHM_MQ_DETACHED)
        {
            // Reset state for retry
            mqh->mqh_partial_bytes = 0;
            mqh->mqh_length_word_complete = false;
            return res;
        }

        mqh->mqh_partial_bytes += bytes_written;
        if (mqh->mqh_partial_bytes >= sizeof(Size))
        {
            mqh->mqh_partial_bytes = 0;
            mqh->mqh_length_word_complete = true;
        }

        if (res != SHM_MQ_SUCCESS)
            return res;
    }

    // Send the actual data from I/O vectors
    offset = mqh->mqh_partial_bytes;
    do
    {
        // Skip to the right I/O vector and offset
        while (which_iov < iovcnt && offset >= iov[which_iov].len)
        {
            offset -= iov[which_iov].len;
            which_iov++;
        }

        if (which_iov >= iovcnt)
            break;

        Size chunksize = iov[which_iov].len - offset;

        // Handle alignment requirements for non-final chunks
        if (which_iov + 1 < iovcnt && offset + MAXIMUM_ALIGNOF > iov[which_iov].len)
        {
            // Copy small unaligned data to aligned buffer
            char tmpbuf[MAXIMUM_ALIGNOF];
            int j = 0;
            // [alignment copying logic omitted for brevity]
            shm_mq_result res = shm_mq_send_bytes(mqh, j, tmpbuf, nowait, &bytes_written);
        }
        else
        {
            // Align chunk size for non-final chunks
            if (which_iov + 1 < iovcnt)
                chunksize = MAXALIGN_DOWN(chunksize);

            shm_mq_result res = shm_mq_send_bytes(mqh, chunksize,
                                                 &iov[which_iov].data[offset],
                                                 nowait, &bytes_written);
        }

        if (res == SHM_MQ_DETACHED)
        {
            // Reset state for retry
            mqh->mqh_length_word_complete = false;
            mqh->mqh_partial_bytes = 0;
            return res;
        }

        mqh->mqh_partial_bytes += bytes_written;
        offset += bytes_written;

        if (res != SHM_MQ_SUCCESS)
            return res;

    } while (mqh->mqh_partial_bytes < nbytes);

    // Reset for next message
    mqh->mqh_partial_bytes = 0;
    mqh->mqh_length_word_complete = false;

    // Notify receiver if needed (force_flush or >1/4 ring size written)
    if (force_flush || mqh->mqh_send_pending > (mq->mq_ring_size >> 2))
    {
        shm_mq_inc_bytes_written(mq, mqh->mqh_send_pending);
        PGPROC *receiver = mq->mq_receiver;
        if (receiver != NULL)
            SetLatch(&receiver->procLatch);
        mqh->mqh_send_pending = 0;
    }

    return SHM_MQ_SUCCESS;
}
```