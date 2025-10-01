# sts_flush_chunk

## Location
[src/backend/utils/sort/sharedtuplestore.c:196-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L196-L212)

## Overview
Flushes the current write chunk buffer to the backing file and resets the buffer for the next chunk of data.

## Definition

```c
static void
sts_flush_chunk(SharedTuplestoreAccessor *accessor)
```
## Detailed Description
The  function writes the current write chunk buffer to the backing BufFile and prepares for the next chunk. It calculates the chunk size based on  and , writes the entire chunk to the file, clears the buffer with zeros, resets the write pointer to the beginning of the chunk's data area, and updates the participant's page count.

This function is called internally when the current write chunk becomes full and needs to be persisted to disk, allowing the buffer to be reused for additional tuple data.

## Parameters / Member Variables
- : SharedTuplestoreAccessor containing the write state and buffer to flush

## Dependencies
- Functions called/Symbols referenced:
  - [SharedTuplestoreAccessor](../S/SharedTuplestoreAccessor.md) (struct type)
  - STS_CHUNK_PAGES (constant)
  - [BufFileWrite](../B/BufFileWrite.md)
  - [write_chunk](../w/write_chunk.md) (accessor field)
- Called from (representative examples):
  - [sts_end_write](sts_end_write.md)
  - [sts_puttuple](sts_puttuple.md)

## Notes and Other Information
- This is a static (internal) function not exposed in the public API
- The function writes exactly  bytes regardless of how much data is actually in the buffer
- After flushing, the write chunk buffer is zeroed out for the next use
- The write pointer is reset to point to the beginning of the data area in the chunk
- The participant's page count is incremented by  to track total data written
- This function is part of the buffered writing mechanism that allows efficient tuple storage in chunks

## Simplified Source

```c
static void sts_flush_chunk(SharedTuplestoreAccessor *accessor) {
    // Calculate chunk size in bytes
    size_t size = STS_CHUNK_PAGES * BLCKSZ;

    // Write the entire chunk buffer to file
    BufFileWrite(accessor->write_file, accessor->write_chunk, size);

    // Clear the buffer for reuse
    memset(accessor->write_chunk, 0, size);

    // Reset write pointer to start of data area
    accessor->write_pointer = &accessor->write_chunk->data[0];

    // Update page count for this participant
    accessor->sts->participants[accessor->participant].npages += STS_CHUNK_PAGES;
}
```