# bbstreamer_plain_writer_content

## Location
[src/bin/pg_basebackup/bbstreamer_file.c:104-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_file.c#L104-L130)

## Overview
Handles the content processing phase of the plain writer base backup streamer by writing received data directly to the output file.

## Definition


## Detailed Description
This is the content processing callback function for the plain writer streamer operations. It receives data chunks from the base backup streaming pipeline and writes them directly to the output file without any transformation. The function performs error handling for write operations and sets appropriate errno values when disk space issues are encountered.

The function is part of the bbstreamer_plain_writer_ops operations table and is called during the content phase of base backup streaming. It casts the generic bbstreamer pointer to the specific bbstreamer_plain_writer type to access the file handle and pathname.

## Parameters / Member Variables
- : Generic bbstreamer pointer, cast internally to bbstreamer_plain_writer
- : Archive member information (unused in this implementation)
- : Pointer to the data buffer to be written to the file
- : Number of bytes to write from the data buffer
- : Archive context information (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - fwrite (standard C library file writing)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting)
  - [bbstreamer_plain_writer](bbstreamer_plain_writer.md) (struct type for casting)
  - bbstreamer_member (parameter type)
  - [bbstreamer_archive_context](bbstreamer_archive_context.md) (parameter type)

- Called from (representative examples):
  - Referenced in bbstreamer_plain_writer_ops.content function pointer
  - Invoked by the base backup streaming framework

## Notes and Other Information
- This is a static function, only accessible within the bbstreamer_file.c module
- Returns early if len is 0 to avoid unnecessary fwrite calls
- Uses errno handling to differentiate between write errors and disk space issues
- Sets errno to ENOSPC when fwrite fails but errno is not set, assuming disk space problems
- The member and context parameters are not used by this plain writer implementation
- Part of the callback-based streaming architecture where different streamers implement different processing strategies