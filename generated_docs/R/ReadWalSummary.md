# ReadWalSummary

## Location
[src/backend/backup/walsummary.c:273-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/walsummary.c#L273-L293)

## Overview
A data read callback function designed for use with CreateBlockRefTableReader to read WAL summary file data in chunks.

## Definition
int ReadWalSummary(void *wal_summary_io, void *data, int length)

## Detailed Description
This function serves as a callback for reading data from WAL summary files. It acts as an abstraction layer between the block reference table reader and the actual file I/O operations. The function reads a specified number of bytes from a WAL summary file starting at the current file position and updates the position pointer accordingly.

The function uses PostgreSQL's File API for reading and includes proper error handling with detailed error messages. It tracks the current file position in the WalSummaryIO structure and advances it by the number of bytes actually read.

## Parameters / Member Variables
- wal_summary_io: A void pointer to a WalSummaryIO structure containing file handle and position information
- data: A void pointer to the buffer where the read data will be stored
- length: The maximum number of bytes to read from the file

## Dependencies
- Functions called/Symbols referenced:
  - [WalSummaryIO](../W/WalSummaryIO.md) (structure type)
  - [FileRead](../F/FileRead.md) (PostgreSQL file I/O function)
  - [FilePathName](../F/FilePathName.md) (PostgreSQL file utility function)
  - ereport (PostgreSQL error reporting function)
  - [errcode_for_file_access](../e/errcode_for_file_access.md) (PostgreSQL error code function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message function)
- Called from:
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)
  - [pg_wal_summary_contents](../p/pg_wal_summary_contents.md)

## Notes and Other Information
- Returns the actual number of bytes read, which may be less than requested if end-of-file is reached
- Uses WAIT_EVENT_WAL_SUMMARY_READ as the wait event type for monitoring purposes
- Automatically updates the file position in the WalSummaryIO structure after each read
- Throws an ERROR if the file read operation fails, providing the filename in the error message
- This callback pattern allows the block reference table reader to be decoupled from specific file I/O implementations

## Simplified Source

```c
int
ReadWalSummary(void *wal_summary_io, void *data, int length)
{
    WalSummaryIO *io = wal_summary_io;
    int nbytes;

    // Read data from file at current position
    nbytes = FileRead(io->file, data, length, io->filepos,
                     WAIT_EVENT_WAL_SUMMARY_READ);

    if (nbytes < 0)
        ereport(ERROR,
               (errcode_for_file_access(),
                errmsg("could not read file \"%s\": %m",
                       FilePathName(io->file))));

    // Update file position for next read
    io->filepos += nbytes;
    return nbytes;
}
```