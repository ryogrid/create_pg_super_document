# WriteWalSummary

## Location
[src/backend/backup/walsummary.c:294-321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/walsummary.c#L294-L321)

## Overview
A data write callback function designed for use with WriteBlockRefTable to write WAL summary file data to disk with comprehensive error handling.

## Definition
int WriteWalSummary(void *wal_summary_io, void *data, int length)

## Detailed Description
This function serves as a callback for writing data to WAL summary files. It acts as an abstraction layer between the block reference table writer and the actual file I/O operations. The function writes a specified number of bytes to a WAL summary file at the current file position and updates the position pointer accordingly.

The function includes robust error handling with two levels of validation:
1. Checks for I/O errors during the write operation
2. Validates that the complete requested data was written (partial writes are treated as errors)

This comprehensive error checking ensures data integrity for WAL summary files, which are critical for incremental backup operations.

## Parameters / Member Variables
- wal_summary_io: A void pointer to a WalSummaryIO structure containing file handle and position information
- data: A void pointer to the buffer containing data to be written
- length: The number of bytes to write to the file

## Dependencies
- Functions called/Symbols referenced:
  - [WalSummaryIO](WalSummaryIO.md) (structure type)
  - [FileWrite](../F/FileWrite.md) (PostgreSQL file I/O function)
  - [FilePathName](../F/FilePathName.md) (PostgreSQL file utility function)
  - ereport (PostgreSQL error reporting function)
  - [errcode_for_file_access](../e/errcode_for_file_access.md) (PostgreSQL error code function)
  - [errmsg](../e/errmsg.md) (PostgreSQL error message function)
  - [errhint](../e/errhint.md) (PostgreSQL error hint function)
- Called from:
  - [SummarizeWAL](../S/SummarizeWAL.md)

## Notes and Other Information
- Returns the actual number of bytes written, which should always equal the requested length on success
- Uses WAIT_EVENT_WAL_SUMMARY_WRITE as the wait event type for monitoring purposes
- Automatically updates the file position in the WalSummaryIO structure after each write
- Throws an ERROR if the file write operation fails or if a partial write occurs
- Partial writes trigger a helpful hint to check free disk space, which is a common cause of write failures
- This callback pattern allows the block reference table writer to be decoupled from specific file I/O implementations
- The strict validation of complete writes ensures WAL summary file integrity, which is essential for reliable incremental backups

## Simplified Source
```c
int WriteWalSummary(void *wal_summary_io, void *data, int length) {
    WalSummaryIO *io = wal_summary_io;

    // Write data to file at current position
    int nbytes = FileWrite(io->file, data, length, io->filepos, WAIT_EVENT_WAL_SUMMARY_WRITE);

    // Check for write errors
    if (nbytes < 0) {
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not write file \"%s\": %m", FilePathName(io->file))));
    }

    // Ensure complete write (partial writes are errors)
    if (nbytes != length) {
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not write file \"%s\": wrote only %d of %d bytes at offset %u",
                       FilePathName(io->file), nbytes, length, (unsigned) io->filepos),
                errhint("Check free disk space.")));
    }

    // Update file position
    io->filepos += nbytes;
    return nbytes;
}
```