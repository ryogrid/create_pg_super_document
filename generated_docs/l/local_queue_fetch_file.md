# local_queue_fetch_file

## Location
[src/bin/pg_rewind/local_source.c:77-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/local_source.c#L77-L127)

## Overview
A static function that copies an entire file from the local source to the target during pg_rewind operations, with size verification to ensure data integrity.

## Definition
static void local_queue_fetch_file(rewind_source *source, const char *path, size_t len)

## Detailed Description
This function implements the queue_fetch_file method for local sources in the rewind_source interface. It performs a complete file copy operation from the local PostgreSQL data directory to the target location. The function opens the source file, creates/truncates the target file, and copies the contents in chunks using an aligned buffer for optimal I/O performance.

The function includes important safety checks to ensure that the source file size matches the expected length, which is crucial for detecting concurrent modifications during the rewind process. This verification helps maintain data consistency and prevents corruption.

## Parameters / Member Variables
- `source`: Pointer to the rewind_source structure (cast to local_source internally)
- `path`: Relative path to the file within the data directory
- `len`: Expected length of the file in bytes

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - open
  - [pg_fatal](../p/pg_fatal.md)
  - [open_target_file](../o/open_target_file.md)
  - read
  - [write_target_range](../w/write_target_range.md)
  - close
- Called from (representative examples):
  - Via function pointer in rewind_source interface

## Notes and Other Information
- This function is static and only used within the local_source.c file
- It's assigned to the queue_fetch_file function pointer in init_local_source
- Uses PGIOAlignedBlock for optimal I/O performance with aligned memory buffers
- Includes concurrent modification detection by comparing expected vs actual file size
- Opens source file with O_RDONLY | PG_BINARY flags for cross-platform compatibility
- Truncates the target file before writing (open_target_file with true parameter)
- Fatal errors are reported using pg_fatal for consistent error handling
- Uses MAXPGPATH for path buffer sizing to handle maximum PostgreSQL path lengths
- Located in src/bin/pg_rewind/local_source.c:77-127

## Simplified Source

```c
static void
local_queue_fetch_file(rewind_source *source, const char *path, size_t len)
{
    const char *datadir = ((local_source *) source)->datadir;
    PGIOAlignedBlock buf;
    char srcpath[MAXPGPATH];
    int srcfd;
    size_t written_len = 0;

    // Build source file path
    snprintf(srcpath, sizeof(srcpath), "%s/%s", datadir, path);

    // Open source file
    srcfd = open(srcpath, O_RDONLY | PG_BINARY, 0);
    if (srcfd < 0)
        pg_fatal("could not open source file \"%s\": %m", srcpath);

    // Prepare target file
    open_target_file(path, true);

    // Copy file in chunks
    for (;;) {
        ssize_t read_len = read(srcfd, buf.data, sizeof(buf));

        if (read_len < 0)
            pg_fatal("could not read file \"%s\": %m", srcpath);
        else if (read_len == 0)
            break;  // EOF reached

        write_target_range(buf.data, written_len, read_len);
        written_len += read_len;
    }

    // Verify file size matches expectation
    if (written_len != len)
        pg_fatal("size of source file \"%s\" changed concurrently: %d bytes expected, %d copied",
                 srcpath, (int) len, (int) written_len);

    close(srcfd);
}
```