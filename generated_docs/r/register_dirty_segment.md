# register_dirty_segment

## Location
src/backend/storage/smgr/md.c: 1355 - 1398

## Overview
Marks a relation segment as needing fsync by registering it with the sync request system or performing an immediate sync if the request queue is full.

## Definition
```c
static void register_dirty_segment(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg)
```

## Detailed Description
The register_dirty_segment function is responsible for marking a relation segment as dirty and ensuring it will be synchronized to stable storage. It implements a two-tier approach for handling sync requests:

1. First, it attempts to register the sync request with the system's pending operations table or forward it to the checkpointer process
2. If the request queue is full and cannot accept the sync request, it falls back to performing an immediate fsync operation locally

The function creates a FileTag to identify the specific segment and uses RegisterSyncRequest to queue the sync operation. If queueing fails due to a full queue, it performs the sync immediately while tracking IO timing statistics.

This function is critical for maintaining data durability and is called by various write operations to ensure dirty pages are eventually written to disk.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer representing the storage manager relation
- `forknum`: ForkNumber indicating which fork of the relation contains the dirty segment
- `seg`: MdfdVec pointer to the specific segment that needs syncing

## Dependencies
- Functions called/Symbols referenced:
  - INIT_MD_FILETAG
  - SmgrIsTemp
  - RegisterSyncRequest
  - pgstat_prepare_io_time
  - FileSync
  - data_sync_elevel
  - FilePathName
  - pgstat_count_io_op_time
- Called from (representative examples):
  - mdcreate
  - mdextend
  - mdzeroextend
  - mdwritev
  - mdtruncate
  - mdregistersync

## Notes and Other Information
- Static function, only called from within the md.c file
- Includes assertion to ensure temporary relations are never fsync'd
- Falls back to immediate fsync if sync request queue is full, with debug logging
- Tracks IO timing statistics for performance monitoring
- Uses IOCONTEXT_NORMAL for statistical counting when performing immediate fsyncs
- Part of PostgreSQL's write-ahead logging and crash recovery mechanism
- Handles error reporting for failed fsync operations with appropriate error contexts