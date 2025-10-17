# read_block

## Location
[src/bin/pg_combinebackup/reconstruct.c:775-789](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/reconstruct.c#L775-L789)

## Overview
A specialized function that reads exactly one PostgreSQL block (BLCKSZ bytes) from a specific offset in a reconstructed file into a buffer, with comprehensive error handling.

## Definition

```c
static void
read_block(rfile *s, off_t off, uint8 *buffer)
```
## Detailed Description
The  function is designed for precise block-level reading operations within PostgreSQL's backup reconstruction system. It uses the  function to read exactly BLCKSZ bytes from a specified file offset, ensuring atomic read operations that don't affect the file's current position. The function provides robust error handling that distinguishes between complete read failures and partial read scenarios, reporting both the expected and actual number of bytes read.

This function is essential for the block-by-block reconstruction process, where individual PostgreSQL data blocks need to be read from various source files at specific offsets and assembled into the final reconstructed file.

## Parameters / Member Variables
- `*s`: Pointer to an rfile structure containing the file descriptor and metadata for the source file
- `off`: File offset (in bytes) where the block should be read from
- `*buffer`: Pointer to a buffer that will receive exactly BLCKSZ bytes of data
## Dependencies
- Functions called/Symbols referenced:
  - pg_pread (PostgreSQL wrapper for positioned read operations)
  - pg_fatal (PostgreSQL error reporting function)
  - BLCKSZ (PostgreSQL block size constant, typically 8KB)
  - rfile (struct type for reconstructed file handling)

- Called from:
  - write_reconstructed_file (called twice: lines 683, 718)

## Notes and Other Information
- This is a static function within the pg_combinebackup reconstruction module
- Uses pg_pread instead of standard read to perform positioned reads without affecting the file pointer
- The function expects the buffer to have space for exactly BLCKSZ bytes
- Provides detailed error messages including the filename, offset, and actual vs expected byte counts
- Essential for the incremental backup reconstruction process where blocks are read from multiple source files
- Error handling uses pg_fatal which terminates the program immediately on any read errors
- The offset parameter allows random access to blocks within large backup files

## Simplified Source

```c
static void read_block(rfile *s, off_t off, uint8 *buffer)
{
    int rb;

    // Read exactly BLCKSZ bytes from the specified offset
    rb = pg_pread(s->fd, buffer, BLCKSZ, off);
    if (rb != BLCKSZ) {
        if (rb < 0)
            pg_fatal("could not read from file \"%s\": %m", s->filename);
        else
            pg_fatal("could not read from file \"%s\", offset %llu: read %d of %d",
                    s->filename, (unsigned long long) off, rb, BLCKSZ);
    }
}
```