# bbsink_gzip_begin_backup

## Location
[src/backend/backup/basebackup_gzip.c:94-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_gzip.c#L94-L113)

## Overview
Initializes the gzip compression sink for the beginning of a base backup operation by allocating its own buffer and configuring the next sink in the chain.

## Definition
```c
static void bbsink_gzip_begin_backup(bbsink *sink)
```

## Detailed Description
This function serves as the begin_backup callback for the gzip compression sink. It performs essential initialization tasks required before the backup process begins. The function allocates a dedicated buffer for the sink using palloc(), which is necessary because the compressed output data will be different from the input data received by this sink.

The function then calls bbsink_begin_backup() on the next sink in the chain, passing along the sink's state and buffer length. This ensures that the entire sink chain is properly initialized for the backup operation. The comment notes that deflate() doesn't require the output buffer to be of any particular size, so the same buffer size is used for both input and output.

## Parameters / Member Variables
- `sink`: The bbsink structure representing this gzip compression sink

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - bbsink_begin_backup (forwards initialization to next sink)
- Called from (representative examples):
  - Used as callback function in bbsink_gzip_ops structure

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit
- Allocates its own buffer because compressed output differs from input data
- Buffer size matches the input buffer size since deflate() is flexible with output buffer sizing
- Part of the bbsink interface implementation for gzip compression
- Ensures proper initialization of the entire sink chain