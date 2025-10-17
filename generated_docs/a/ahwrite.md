# ahwrite

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1827-1873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1827-L1873)

## Overview
A versatile output function that writes data to various destinations including large object buffers, database connections, custom output handlers, or compressed files.

## Definition

```c
void
ahwrite(const void *ptr, size_t size, size_t nmemb, ArchiveHandle *AH)
```
## Detailed Description
The  function serves as the central output mechanism for the PostgreSQL archiver, routing data to appropriate destinations based on the current context. When writing large objects, it manages a buffer and calls  when the buffer fills. For custom formats, it delegates to custom output functions. When restoring directly to a database, it executes SQL commands via . Otherwise, it writes to compressed file handles using the appropriate compression method.

## Parameters / Member Variables
- `*ptr`: Pointer to the data to be written
- `size`: Size of each element in bytes
- `nmemb`: Number of elements to write
- `*AH`: Archive handle containing output context and destination information
## Dependencies
- Functions called/Symbols referenced:
  - [dump_lo_buf](../d/dump_lo_buf.md)
  - [RestoringToDB](../R/RestoringToDB.md)
  - [ExecuteSqlCommandBuf](../E/ExecuteSqlCommandBuf.md)
  - [CompressFileHandle](../C/CompressFileHandle.md)
  - WRITE_ERROR_EXIT
- Called from (representative examples):
  - [ReadDataFromArchiveGzip](../R/ReadDataFromArchiveGzip.md)
  - [ReadDataFromArchiveLZ4](../R/ReadDataFromArchiveLZ4.md)
  - [ReadDataFromArchiveNone](../R/ReadDataFromArchiveNone.md)
  - [ReadDataFromArchiveZstd](../R/ReadDataFromArchiveZstd.md)
  - [ahprintf](ahprintf.md)
  - appendByteaLiteralAHX
  - [_PrintFileData](../P/_PrintFileData.md)
  - [_WriteData](../W/_WriteData.md)

## Notes and Other Information
- Handles multiple output modes: LO buffering, custom output, direct database execution, and compressed file output
- Uses a buffering mechanism for large objects to optimize write operations
- Verifies that all requested bytes are written and exits with error if not
- Central routing function used by various compression and format handlers throughout the pg_dump system

## Simplified Source

```c
void
ahwrite(const void *ptr, size_t size, size_t nmemb, ArchiveHandle *AH)
{
    int bytes_written = 0;

    if (AH->writingLO)
    {
        // Large object buffering - copy data to LO buffer
        size_t remaining = size * nmemb;

        while (AH->lo_buf_used + remaining > AH->lo_buf_size)
        {
            size_t avail = AH->lo_buf_size - AH->lo_buf_used;

            memcpy((char *) AH->lo_buf + AH->lo_buf_used, ptr, avail);
            ptr = (const void *) ((const char *) ptr + avail);
            remaining -= avail;
            AH->lo_buf_used += avail;
            dump_lo_buf(AH);
        }

        memcpy((char *) AH->lo_buf + AH->lo_buf_used, ptr, remaining);
        AH->lo_buf_used += remaining;
        bytes_written = size * nmemb;
    }
    else if (AH->CustomOutPtr)
    {
        // Custom output handler
        bytes_written = AH->CustomOutPtr(AH, ptr, size * nmemb);
    }
    else if (RestoringToDB(AH))
    {
        // Direct database execution
        bytes_written = ExecuteSqlCommandBuf(&AH->public, (const char *) ptr, size * nmemb);
    }
    else
    {
        // Compressed file output
        CompressFileHandle *CFH = (CompressFileHandle *) AH->OF;
        CFH->write_func(ptr, size * nmemb, CFH);
        bytes_written = size * nmemb;
    }

    // Verify all bytes were written
    if (bytes_written != size * nmemb)
        WRITE_ERROR_EXIT;
}
```