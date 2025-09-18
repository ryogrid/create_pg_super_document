# XLogRecGetBlockTagExtended

## Location
[src/backend/access/transam/xlogreader.c:2007-2034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L2007-L2034)

## Overview
XLogRecGetBlockTagExtended retrieves block identification information and optional prefetch buffer details from a WAL record's block reference, with graceful handling of missing references.

## Definition


## Detailed Description
XLogRecGetBlockTagExtended provides comprehensive access to block reference information within a WAL record. The function first checks if the specified block reference exists using XLogRecHasBlockRef, and if so, extracts the relation file locator, fork number, block number, and optionally the prefetch buffer information. This extended version offers more control than XLogRecGetBlockTag by returning a boolean success indicator rather than throwing an error, and provides access to prefetch buffer information which can be used for optimization purposes.

## Parameters / Member Variables
- `record`: XLogReaderState containing the decoded WAL record
- `block_id`: ID of the block reference within the record (0-based)
- `rlocator`: Output parameter for the relation file locator information (optional, can be NULL)
- `forknum`: Output parameter for the fork number (optional, can be NULL)
- `blknum`: Output parameter for the block number within the relation (optional, can be NULL)
- `prefetch_buffer`: Output parameter for the prefetch buffer information (optional, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecHasBlockRef
  - DecodedBkpBlock (structure access)
- Called from (representative examples):
  - [heap_xlog_update](../h/heap_xlog_update.md)
  - [btree_xlog_split](../b/btree_xlog_split.md)
  - [XLogRecGetBlockRefInfo](XLogRecGetBlockRefInfo.md)
  - [XLogRecGetBlockTag](XLogRecGetBlockTag.md)
  - [xlog_block_info](../x/xlog_block_info.md)
  - [verifyBackupPageConsistency](../v/verifyBackupPageConsistency.md)
  - XLogReadBufferForRedoExtended

## Notes and Other Information
- Returns true if the block reference exists, false otherwise (no error thrown)
- All output parameters are optional and can be NULL if that information is not needed
- Provides access to prefetch buffer information, which can be used for performance optimization
- More flexible than XLogRecGetBlockTag as it handles missing block references gracefully
- The prefetch_buffer parameter is unique to this extended version and not available in the basic version
- Commonly used in scenarios where block reference existence is uncertain or when prefetch information is needed