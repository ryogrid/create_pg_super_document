# tarGets

## Location
[src/bin/pg_dump/pg_backup_tar.c:418-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_tar.c#L418-L461)

## Overview
Reads a line of text from a tar archive member, similar to the standard library fgets() function, but operates on TAR_MEMBER structures.

## Definition
```c
static char *tarGets(char *buf, size_t len, TAR_MEMBER *th)
```

## Detailed Description
The tarGets function provides a line-oriented reading interface for files within tar archives. It reads characters one by one using _tarReadRaw until it encounters a newline character or reaches the specified buffer length limit. The function respects the logical file boundaries within the tar archive by ensuring it doesn't read past the file's end position.

The function handles end-of-file conditions gracefully, returning NULL if no data could be read, or returning the buffer with a null-terminated string if any data was successfully read. It also tracks the current position within the file for subsequent read operations.

## Parameters / Member Variables
- `buf`: Buffer to store the read line
- `len`: Maximum number of characters to read (including null terminator)
- `th`: TAR_MEMBER pointer representing the file to read from

## Dependencies
- Functions called/Symbols referenced:
  - [_tarReadRaw](_tarReadRaw.md)
  - strlen
- Called from (representative examples):
  - lclTocEntry

## Notes and Other Information
- Stops reading at newline character or buffer length limit, whichever comes first
- Returns NULL if EOF is encountered before any data is read
- Always null-terminates the returned string
- Updates the file position (th->pos) after successful reads
- Respects logical file boundaries within the tar archive (th->fileLen)
- Provides fgets()-like semantics for tar archive file reading

## Simplified Source

```c
static char *tarGets(char *buf, size_t len, TAR_MEMBER *th)
{
    size_t cnt = 0;
    char c = ' ';
    int eof = 0;

    // Don't read past logical file end
    if (len > (th->fileLen - th->pos))
        len = th->fileLen - th->pos;

    // Read characters until newline or buffer full
    while (cnt < len && c != '\n') {
        if (_tarReadRaw(th->AH, &c, 1, th, NULL) <= 0) {
            eof = 1;
            break;
        }
        buf[cnt++] = c;
    }

    // Handle EOF and null-terminate
    char *result;
    if (eof && cnt == 0) {
        result = NULL;  // No data read
    } else {
        buf[cnt++] = '\0';
        result = buf;
    }

    // Update position
    if (result) {
        size_t actual_len = strlen(result);
        th->pos += actual_len;
    }

    return result;
}
```