# hash_xlog_init_meta_page

## Location
[src/backend/access/hash/hash_xlog.c:27-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L27-L62)

## Overview
Replays the initialization of a hash index metapage during WAL recovery, restoring the metapage to its proper state after a crash or restart.

## Definition

```c
static void
hash_xlog_init_meta_page(XLogReaderState *record)
```
## Detailed Description
This function is responsible for replaying WAL records that represent the initialization of a hash index's metapage. During normal operation, when a hash index is created, the metapage initialization is logged to WAL. During recovery, this function reconstructs the metapage from the WAL record data.

The function extracts the metapage initialization parameters from the WAL record, creates and initializes the metapage buffer using the stored parameters (number of tuples, process ID, fill factor), sets the LSN, and handles special cases for init forks by ensuring synchronization between shared buffers and disk state.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record with metapage initialization data including num_tuples, procid, and ffactor
## Dependencies
- Functions called/Symbols referenced:
  - [xl_hash_init_meta_page](../x/xl_hash_init_meta_page.md) (WAL record structure)
  - XLogRecGetData (extracts record data)
  - [XLogInitBufferForRedo](../X/XLogInitBufferForRedo.md) (initializes buffer for recovery)
  - [_hash_init_metabuffer](_hash_init_metabuffer.md) (initializes the metapage buffer)
  - [XLogRecGetBlockTag](../X/XLogRecGetBlockTag.md) (gets block information)
  - INIT_FORKNUM (fork number constant)
  - [FlushOneBuffer](../F/FlushOneBuffer.md) (flushes buffer to disk)
- Called from:
  - [hash_redo](hash_redo.md) (main hash WAL replay function)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery subsystem
- Special handling is required for init forks to maintain synchronization between shared buffers and disk state
- The function ensures the metapage LSN is properly set for recovery consistency
- Init fork handling is necessary because create index operations don't log a full page image of the metapage