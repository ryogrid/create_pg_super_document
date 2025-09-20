# XLogRecGetBlockTag

## Location
[src/backend/access/transam/xlogreader.c:1981-2006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L1981-L2006)

## Overview
XLogRecGetBlockTag extracts block identification information from a WAL record's block reference, providing a simplified interface that requires the block reference to exist.

## Definition

```c
void
XLogRecGetBlockTag(XLogReaderState *record, uint8 block_id,
				   RelFileLocator *rlocator, ForkNumber *forknum,
				   BlockNumber *blknum)
```
## Detailed Description
XLogRecGetBlockTag is a convenience wrapper around XLogRecGetBlockTagExtended that retrieves block identification information for a specific block reference within a WAL record. Unlike its extended counterpart, this function assumes the block reference must exist and will throw an error if it doesn't. The function extracts the relation file locator, fork number, and block number for the specified block ID, which are essential for identifying which specific block the WAL record operates on.

## Parameters / Member Variables
- `record`: XLogReaderState containing the decoded WAL record
- `block_id`: ID of the block reference within the record (0-based)
- `rlocator`: Output parameter for the relation file locator information
- `forknum`: Output parameter for the fork number (main, FSM, VM, etc.)
- `blknum`: Output parameter for the block number within the relation

## Dependencies
- Functions called/Symbols referenced:
  - [XLogRecGetBlockTagExtended](XLogRecGetBlockTagExtended.md)
  - elog/pg_fatal (for error reporting)
- Called from (representative examples):
  - [brin_xlog_revmap_extend](../b/brin_xlog_revmap_extend.md)
  - [gistRedoDeleteRecord](../g/gistRedoDeleteRecord.md)
  - [hash_xlog_init_meta_page](../h/hash_xlog_init_meta_page.md)
  - [heap_xlog_prune_freeze](../h/heap_xlog_prune_freeze.md)
  - [btree_xlog_split](../b/btree_xlog_split.md)
  - [DecodeInsert](../D/DecodeInsert.md)
  - XLogRecHasBlockData

## Notes and Other Information
- This is a non-optional version of XLogRecGetBlockTagExtended - it will error if the block reference doesn't exist
- Uses different error reporting mechanisms for backend (elog) vs frontend (pg_fatal) contexts
- Commonly used in WAL replay functions that expect specific block references to be present
- Does not provide access to prefetch buffer information (use XLogRecGetBlockTagExtended for that)
- Essential for WAL record processing in recovery and logical replication scenarios