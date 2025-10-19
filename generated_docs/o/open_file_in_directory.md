# open_file_in_directory

## Location
[src/bin/pg_waldump/pg_waldump.c:188-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L188-L209)

## Overview
Opens a file with a specified filename within a given directory and returns a read-only file descriptor for use by PostgreSQL's WAL dump utility.

## Definition

```c
struct dirent *xlde;
```
## Detailed Description
This function constructs a file path by combining a directory path and filename, then attempts to open the file in read-only mode with binary access. The function is designed to handle file opening operations safely within the pg_waldump utility context. It uses PostgreSQL's standard error handling approach and will terminate the program with a fatal error if the file cannot be opened (except for the case where the file simply doesn't exist, indicated by ENOENT).

The function ensures the directory parameter is valid through an assertion and constructs the full file path using snprintf to prevent buffer overflows. It opens files with the O_RDONLY and PG_BINARY flags to ensure proper binary file handling across different platforms.

## Parameters / Member Variables
- : A null-terminated string specifying the directory path where the target file is located
- : A null-terminated string specifying the filename to open within the directory

## Dependencies
- Functions called/Symbols referenced:
  - open (system call)
  - PG_BINARY (PostgreSQL binary flag constant)
- Called from (representative examples):
  - [search_directory](../s/search_directory.md)
  - [WALDumpOpenSegment](../W/WALDumpOpenSegment.md)
  - [main](../m/main.md)

## Notes and Other Information
- Returns -1 if the file doesn't exist (ENOENT error) or on successful opening returns a valid file descriptor
- Uses MAXPGPATH constant to limit the constructed file path length
- Part of the pg_waldump utility for analyzing PostgreSQL Write-Ahead Log files
- The function will call pg_fatal() and terminate the program if file opening fails for reasons other than file non-existence

## Simplified Source

```c
static int
open_file_in_directory(const char *directory, const char *fname)
{
    char fpath[MAXPGPATH];

    // Construct full file path
    snprintf(fpath, MAXPGPATH, "%s/%s", directory, fname);

    // Open file in read-only binary mode
    int fd = open(fpath, O_RDONLY | PG_BINARY, 0);

    // Fatal error if open fails (except for file not found)
    if (fd < 0 && errno != ENOENT)
        pg_fatal("could not open file \"%s\": %m", fname);

    return fd;
}
```