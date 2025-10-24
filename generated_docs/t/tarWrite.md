# tarWrite

## Location
[src/bin/pg_dump/pg_backup_tar.c:529-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L529-L539)

## Overview
A low-level utility function that writes binary data to a tar archive member file handle, maintaining position tracking for the tar member.

## Definition
static size_t tarWrite(const void *buf, size_t len, TAR_MEMBER *th)

## Detailed Description
The tarWrite function serves as a wrapper around the standard C library fwrite() function, providing tar-specific functionality for writing data to archive members. It writes the specified buffer to the file handle associated with the given TAR_MEMBER structure and updates the member's position counter to track the current write position within the tar member. This function is essential for building tar archive entries by ensuring proper data writing and position tracking.

## Parameters / Member Variables
- : Pointer to the data buffer to be written to the tar member
- : Number of bytes to write from the buffer
- : Pointer to TAR_MEMBER structure containing the file handle and position information

## Dependencies
- Functions called/Symbols referenced:
  - fwrite (standard C library function)
  - [TAR_MEMBER](../T/TAR_MEMBER.md) (structure type)
- Called from (representative examples):
  - [_WriteData](../W/_WriteData.md)
  - [_WriteByte](../W/_WriteByte.md)
  - [_WriteBuf](../W/_WriteBuf.md)
  - [_scriptOut](../s/_scriptOut.md)
  - [tarPrintf](tarPrintf.md)

## Notes and Other Information
- Returns the number of bytes actually written, which may be less than requested if an error occurs
- Updates the position counter (th->pos) to maintain accurate position tracking within the tar member
- This is a static function, meaning it's only accessible within the pg_backup_tar.c file
- Serves as the fundamental building block for all tar archive data writing operations in pg_dump

## Simplified Source

```c
static size_t tarWrite(const void *buf, size_t len, TAR_MEMBER *th)
{
    // Write data to file handle
    size_t result = fwrite(buf, 1, len, th->nFH);

    // Update position tracking
    th->pos += result;

    return result;
}
```