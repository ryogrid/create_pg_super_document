# extractPageInfo

## Location
src/bin/pg_rewind/parsexlog.c: 389 - 483

## Overview
Analyzes individual WAL records to identify which data blocks are modified and adds them to the page map used by pg_rewind for determining file synchronization requirements.

## Definition


## Detailed Description
This function is the core page analysis component of pg_rewind's WAL processing system. It examines individual WAL records to determine which data blocks have been modified and need to be considered during the rewind operation. The function implements specific handling logic for various types of WAL records, categorizing them based on their impact on data files.

The function recognizes several categories of WAL records: database creation/deletion operations (which can be safely ignored as entire databases are handled separately), storage manager operations like file creation and truncation (also safely ignored), and transaction commit/abort records (which may contain dropped relation information but don't require special page tracking).

For records that modify actual data blocks, the function iterates through all blocks referenced by the WAL record, extracting the RelFileLocator, fork number, and block number. It focuses only on the main fork of relations, as other forks (visibility map, free space map) are copied in their entirety. For each main fork block, it calls process_target_wal_block_change() to register the block in the page map.

## Parameters / Member Variables
- : XLogReaderState containing the current WAL record to analyze

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetRmid
  - XLogRecGetInfo
  - XLogRecMaxBlockId
  - XLogRecGetBlockTagExtended
  - RmgrName
  - process_target_wal_block_change
  - RmgrId
  - RelFileLocator
  - ForkNumber
  - BlockNumber
- Called from (representative examples):
  - extractPageMap (in src/bin/pg_rewind/parsexlog.c:100)

## Notes and Other Information
- Implements selective processing based on WAL record type (rmid and rminfo)
- Only tracks changes to MAIN_FORKNUM - other forks are copied completely
- Includes safety logic for database and storage manager operations that don't require block-level tracking  
- Contains fatal error handling for unrecognized special relation update records
- Critical for building the accurate page map that drives pg_rewind's selective file copying
- The function's logic determines which blocks need to be copied vs. which can be safely ignored
- Works in conjunction with process_target_wal_block_change() to maintain the global page map