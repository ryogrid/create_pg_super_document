# bbstreamer_plain_writer_free

## Location
[src/bin/pg_basebackup/bbstreamer_file.c:149-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_file.c#L149-L182)

## Overview
This function deallocates memory associated with a plain file writer bbstreamer after ensuring the file is properly closed and no downstream streamers are attached.

## Definition

```c
static void
bbstreamer_plain_writer_free(bbstreamer *streamer)
```
## Detailed Description
The `bbstreamer_plain_writer_free` function is responsible for freeing memory allocated to a `bbstreamer_plain_writer` structure. Before deallocating memory, it performs assertions to ensure proper cleanup state: the file should already be closed (verified by `!should_close_file` assertion) and no downstream bbstreamers should be connected (verified by checking `bbs_next == NULL`). The function then frees the pathname string and the streamer structure itself using PostgreSQL's `pfree` function. This is part of the resource cleanup in PostgreSQL's backup streaming system.

## Parameters / Member Variables
- `streamer`: Base bbstreamer pointer that gets cast to `bbstreamer_plain_writer` type for accessing plain writer-specific members

## Dependencies
- Functions called/Symbols referenced:
  - `Assert` (PostgreSQL assertion macro)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - Referenced as callback in `bbstreamer_plain_writer_ops` vtable
  - Called indirectly through bbstreamer cleanup routines

## Notes and Other Information
- This is a static function serving as a callback in the bbstreamer operations structure
- Contains assertions to validate proper cleanup sequence before memory deallocation
- The assertions help catch programming errors where cleanup is not done in the correct order
- Part of PostgreSQL's memory management for backup streaming components
- Must be called only after finalization has completed successfully
- Located in src/bin/pg_basebackup/bbstreamer_file.c:149-160

## Simplified Source

```c
static void
bbstreamer_plain_writer_free(bbstreamer *streamer)
{
    bbstreamer_plain_writer *writer = (bbstreamer_plain_writer *) streamer;

    // Verify cleanup state before freeing
    Assert(!writer->should_close_file);    // File should be closed
    Assert(writer->base.bbs_next == NULL); // No downstream streamers

    // Free allocated memory
    pfree(writer->pathname);
    pfree(writer);
}
```