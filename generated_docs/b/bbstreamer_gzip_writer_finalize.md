# bbstreamer_gzip_writer_finalize

## Location
[src/bin/pg_basebackup/bbstreamer_gzip.c:159-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_gzip.c#L159-L176)

## Overview
Finalizes the gzip compression process by closing the compressed file and performing end-of-archive cleanup for the backup streamer.

## Definition
```c
static void bbstreamer_gzip_writer_finalize(bbstreamer *streamer)
```

## Detailed Description
This static function handles the finalization phase of gzip-compressed backup streaming. It closes the gzip file handle using gzclose(), which automatically flushes any remaining compressed data and closes the underlying file descriptor. The function is designed to work consistently regardless of whether the original file was opened by bbstreamer_gzip_writer_new() or passed in by the caller, because libz always closes the underlying file handle.

Due to libz's behavior of always closing the underlying file descriptor, the constructor uses dup() to duplicate file descriptors, ensuring that the caller's view of file handling remains consistent with other bbstreamer implementations like bbstreamer_plain_writer.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance to finalize (cast to bbstreamer_gzip_writer internally)

## Dependencies
- Functions called/Symbols referenced:
  - gzclose
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - Used as callback through bbstreamer_gzip_writer_ops function pointer table

## Notes and Other Information
- Sets errno to 0 before calling gzclose() to ensure accurate error reporting
- Nullifies the gzfile pointer after successful closure to prevent double-close issues
- Works with both self-opened files and caller-provided file handles due to dup() usage in constructor
- Part of the static callback interface for backup streaming cleanup
- Essential for proper resource cleanup and data integrity in compressed backups

## Simplified Source

```c
static void
bbstreamer_gzip_writer_finalize(bbstreamer *streamer)
{
    bbstreamer_gzip_writer *writer = (bbstreamer_gzip_writer *) streamer;

    // Close compressed file
    errno = 0;  // Clear errno before gzclose
    if (gzclose(writer->gzfile) != 0)
        pg_fatal("could not close compressed file \"%s\": %m", writer->pathname);

    // Clean up state
    writer->gzfile = NULL;
}
```