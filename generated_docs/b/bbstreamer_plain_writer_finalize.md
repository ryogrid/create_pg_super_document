# bbstreamer_plain_writer_finalize

## Location
[src/bin/pg_basebackup/bbstreamer_file.c:131-148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_file.c#L131-L148)

## Overview
This function finalizes a plain file writer bbstreamer by closing the file if it was opened by the streamer itself, and cleaning up file-related state.

## Definition

```c
static void
bbstreamer_plain_writer_finalize(bbstreamer *streamer)
```
## Detailed Description
The `bbstreamer_plain_writer_finalize` function is the finalization callback for plain file writer bbstreamers. It performs end-of-archive cleanup when writing to a plain file. The function only closes the file if the streamer originally opened it (indicated by the `should_close_file` flag) - it does not close files that were provided by the caller. After closing the file (if needed), it cleans up the internal state by setting the file pointer to NULL and the `should_close_file` flag to false. This is part of the bbstreamer operations vtable pattern used in PostgreSQL's backup streaming architecture.

## Parameters / Member Variables
- `streamer`: Base bbstreamer pointer that gets cast to `bbstreamer_plain_writer` type to access the plain writer-specific fields

## Dependencies
- Functions called/Symbols referenced:
  - `fclose` (standard C library function)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting function)
- Called from (representative examples):
  - Referenced as callback in `bbstreamer_plain_writer_ops` vtable
  - Called indirectly through bbstreamer finalization

## Notes and Other Information
- This is a static function serving as a callback in the bbstreamer operations structure
- The function includes error handling for file close failures using `pg_fatal`
- Part of the cleanup phase in PostgreSQL's backup streaming system
- The `should_close_file` flag ensures that files opened by the caller are not inadvertently closed
- Located in src/bin/pg_basebackup/bbstreamer_file.c:131-148