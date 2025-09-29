# BufFileWrite

## Location
[src/backend/storage/file/buffile.c:676-719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L676-L719)

## Overview
Writes data to a buffered file, using an internal buffer for efficiency and handling file segmentation automatically.

## Definition
```c
void BufFileWrite(BufFile *file, const void *ptr, size_t size)
```

## Detailed Description
BufFileWrite writes data to a BufFile through an internal buffer system. It functions similarly to fwrite() but with PostgreSQL-specific error handling and assumes 1-byte element size. The function uses a BLCKSZ-sized buffer to collect writes before flushing to disk, providing efficient I/O for both small and large writes.

The function handles writes by:
1. Copying data to the internal buffer when there's space available
2. Flushing the buffer to disk when it becomes full (reaches BLCKSZ)
3. Handling transitions from reading to writing by adjusting buffer state
4. Managing the dirty flag to track when buffer contents need to be written
5. Automatically extending the buffer's valid byte count (nbytes) as needed

The write process is entirely buffered - data is copied to the internal buffer and the file is marked as dirty, but actual disk I/O is deferred until the buffer is full or explicitly flushed.

## Parameters / Member Variables
- `file`: Pointer to the BufFile structure representing the target buffered file
- `ptr`: Pointer to the source data buffer to write from
- `size`: Number of bytes to write from the source buffer

## Dependencies
- Functions called/Symbols referenced:
  - [BufFileDumpBuffer](BufFileDumpBuffer.md) (internal function to flush buffer contents to disk)
- Called from (representative examples):
  - [WriteTempFileBlock](../W/WriteTempFileBlock.md) (GIST index building operations)
  - [AppendStringToManifest](../A/AppendStringToManifest.md) (backup manifest creation)
  - [ExecHashJoinSaveTuple](../E/ExecHashJoinSaveTuple.md) (hash join tuple spooling)
  - [subxact_info_write](../s/subxact_info_write.md) (logical replication subtransaction state)
  - [stream_write_change](../s/stream_write_change.md) (logical replication streaming)
  - [ltsWriteBlock](../l/ltsWriteBlock.md) (log tape system for sorting)
  - [sts_flush_chunk](../s/sts_flush_chunk.md) (shared tuplestore chunk writing)
  - [writetup_heap](../w/writetup_heap.md) (tuplestore heap writing)

## Notes and Other Information
- Asserts that the file is not read-only before proceeding with write operations
- Sets the dirty flag to indicate the buffer contains unwritten data
- Automatically manages buffer positioning and valid byte counts
- Handles the transition from read mode to write mode by adjusting buffer state
- Uses BLCKSZ as the buffer size limit for flushing decisions
- Does not immediately write to disk - writes are buffered until the buffer fills or is explicitly flushed
- Errors are reported via ereport() using PostgreSQL's error reporting mechanism
- Supports writing data larger than the buffer size by breaking it into chunks
- Maintains file positioning information for proper buffer management and seeking operations

## Simplified Source

```c
void BufFileWrite(BufFile *file, const void *ptr, size_t size)
{
    size_t nthistime;

    Assert(!file->readOnly);

    while (size > 0)
    {
        // If buffer is full, dump it out
        if (file->pos >= BLCKSZ)
        {
            if (file->dirty)
                BufFileDumpBuffer(file);
            else
            {
                // Direct transition from reading to writing
                file->curOffset += file->pos;
                file->pos = 0;
                file->nbytes = 0;
            }
        }

        // Calculate how much to write this time
        nthistime = BLCKSZ - file->pos;
        if (nthistime > size)
            nthistime = size;
        Assert(nthistime > 0);

        // Copy data to buffer
        memcpy(file->buffer.data + file->pos, ptr, nthistime);

        // Update buffer state
        file->dirty = true;
        file->pos += nthistime;
        if (file->nbytes < file->pos)
            file->nbytes = file->pos;

        // Advance to next chunk
        ptr = (const char *) ptr + nthistime;
        size -= nthistime;
    }
}
```