# bbstreamer_gzip_writer_content

## Location
src/bin/pg_basebackup/bbstreamer_gzip.c: 126 - 158

## Overview
Writes archive content data to a gzip-compressed file as part of the backup streaming process, handling compression and error reporting.

## Definition
```c
static void bbstreamer_gzip_writer_content(bbstreamer *streamer, bbstreamer_member *member, const char *data, int len, bbstreamer_archive_context context)
```

## Detailed Description
This static function serves as the content processing callback for the gzip writer bbstreamer. It receives data chunks from the backup streaming pipeline and writes them to the compressed output file using gzwrite(). The function includes comprehensive error handling, automatically detecting disk space issues when write operations fail without setting errno, and provides detailed error messages using the pathname for context.

The function is designed to handle incremental data writing, where backup content arrives in chunks that need to be compressed and written sequentially. It's part of the bbstreamer operation callbacks and should not be called directly by external code.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance (cast to bbstreamer_gzip_writer internally)
- `member`: Information about the current archive member being processed (unused in this function)
- `data`: Pointer to the data buffer to write
- `len`: Number of bytes to write from the data buffer
- `context`: Archive context information (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - gzwrite
  - get_gz_error
  - pg_fatal
- Called from (representative examples):
  - Used as callback through bbstreamer_gzip_writer_ops function pointer table

## Notes and Other Information
- Early returns if len is 0 to avoid unnecessary processing
- Explicitly sets errno to 0 before write operations for accurate error detection
- Assumes ENOSPC (no space left on device) when gzwrite fails without setting errno
- Uses the pathname from the streamer for error reporting context
- Part of the static callback interface, not intended for direct external invocation