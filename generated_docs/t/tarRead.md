# tarRead

## Location
[src/bin/pg_dump/pg_backup_tar.c:511-528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L511-L528)

## Overview
Reads binary data from a tar archive member with automatic boundary checking and position tracking.

## Definition
```c
static size_t tarRead(void *buf, size_t len, TAR_MEMBER *th)
```

## Detailed Description
The tarRead function provides a safe, high-level interface for reading binary data from files within tar archives. It acts as a wrapper around _tarReadRaw, adding important boundary checking to ensure reads don't extend past the logical end of the file within the archive. The function automatically adjusts the read length if the requested amount would exceed the file's boundaries and updates the current position within the file after successful reads.

This function is designed to provide fread()-like semantics for tar archive members, making it easy to integrate with existing code that expects standard file I/O behavior.

## Parameters / Member Variables
- `buf`: Buffer to store the read data
- `len`: Maximum number of bytes to read
- `th`: TAR_MEMBER pointer representing the file to read from

## Dependencies
- Functions called/Symbols referenced:
  - [_tarReadRaw](_tarReadRaw.md)
- Called from (representative examples):
  - [_PrintFileData](../P/_PrintFileData.md)
  - [_LoadLOs](../L/_LoadLOs.md)
  - [_ReadByte](../R/_ReadByte.md)
  - [_ReadBuf](../R/_ReadBuf.md)

## Notes and Other Information
- Automatically prevents reading past the logical file end (th->fileLen)
- Updates the file position (th->pos) after successful reads
- Returns 0 immediately if no data can be read (at or past EOF)
- Provides fread()-like semantics for tar archive file reading
- Serves as the safe, boundary-checked interface to _tarReadRaw
- Used by higher-level archive reading functions for data extraction