# LogicalTapeRead

## Location
[src/backend/utils/sort/logtape.c:928-980](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L928-L980)

## Overview
LogicalTapeRead is a function that reads data from a logical tape, providing buffered sequential access to data stored in temporary files during sorting operations.

## Definition
```c
size_t LogicalTapeRead(LogicalTape *lt, void *ptr, size_t size)
```

## Detailed Description
LogicalTapeRead performs buffered reading from a logical tape data structure. It ensures the tape is in read mode (not writing), initializes the read buffer if necessary, and then reads the requested amount of data into the provided buffer. The function handles partial reads and EOF conditions gracefully, returning the actual number of bytes read which may be less than requested if EOF is encountered.

The function works by:
1. Verifying the tape is not in writing mode
2. Initializing the read buffer if it hasn't been set up yet
3. Reading data in chunks from the internal buffer
4. Refilling the buffer from storage when needed
5. Copying data to the caller's buffer until the request is satisfied or EOF is reached

## Parameters / Member Variables
- `lt`: Pointer to the LogicalTape structure representing the tape to read from
- `ptr`: Destination buffer where the read data will be stored
- `size`: Number of bytes requested to read

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTape](LogicalTape.md) (structure type)
  - [ltsInitReadBuffer](../l/ltsInitReadBuffer.md) (initializes read buffer)
  - [ltsReadFillBuffer](../l/ltsReadFillBuffer.md) (refills buffer from storage)
- Called from (representative examples):
  - [hashagg_batch_read](../h/hashagg_batch_read.md) (in nodeAgg.c for hash aggregation)
  - [getlen](../g/getlen.md) (in tuplesort.c for tuple sorting)
  - LogicalTapeReadExact (wrapper function for exact reads)

## Notes and Other Information
- Returns the actual number of bytes read, which may be less than requested if EOF is encountered
- Early EOF detection is built into the function's design
- The function maintains internal buffering for efficient I/O operations
- Must not be called on a tape that is currently in writing mode
- Part of PostgreSQL's external sorting infrastructure used during large sort operations

## Simplified Source

```c
size_t LogicalTapeRead(LogicalTape *lt, void *ptr, size_t size)
{
    size_t bytes_read = 0;
    size_t chunk_size;

    // Ensure tape is in read mode
    Assert(!lt->writing);

    // Initialize read buffer if needed
    if (lt->buffer == NULL)
        ltsInitReadBuffer(lt);

    // Read data in chunks until request is satisfied or EOF
    while (size > 0) {
        // Refill buffer if empty
        if (lt->pos >= lt->nbytes) {
            if (!ltsReadFillBuffer(lt))
                break; // End of file reached
        }

        // Calculate how much to read from current buffer
        chunk_size = lt->nbytes - lt->pos;
        if (chunk_size > size)
            chunk_size = size;

        // Copy data from buffer to destination
        memcpy(ptr, lt->buffer + lt->pos, chunk_size);

        // Update positions and counters
        lt->pos += chunk_size;
        ptr = (char *) ptr + chunk_size;
        size -= chunk_size;
        bytes_read += chunk_size;
    }

    return bytes_read;
}
```