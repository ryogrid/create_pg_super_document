# brin_redo

## Location
[src/backend/access/brin/brin_xlog.c:309-341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_xlog.c#L309-L341)

## Overview
Main dispatcher function for BRIN (Block Range Index) WAL record replay operations during PostgreSQL crash recovery.

## Definition


## Detailed Description
This function serves as the central dispatcher for all BRIN-related WAL (Write-Ahead Log) replay operations during crash recovery. It examines the operation type encoded in the WAL record and delegates to the appropriate specialized replay function. The function handles six different BRIN operation types:

1. **XLOG_BRIN_CREATE_INDEX**: Index creation operations
2. **XLOG_BRIN_INSERT**: New tuple insertion into summary pages
3. **XLOG_BRIN_UPDATE**: Tuple updates requiring page modifications
4. **XLOG_BRIN_SAMEPAGE_UPDATE**: In-place tuple updates within the same page
5. **XLOG_BRIN_REVMAP_EXTEND**: Extension of the reverse mapping structure
6. **XLOG_BRIN_DESUMMARIZE**: Invalidation of summary information

The function uses a switch statement to efficiently route each WAL record to its corresponding replay handler, ensuring that BRIN indexes are correctly reconstructed during recovery.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record to be replayed, including operation type information and associated data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo: Extract operation information from WAL record
  - [brin_xlog_createidx](brin_xlog_createidx.md): Handle index creation replay
  - [brin_xlog_insert](brin_xlog_insert.md): Handle tuple insertion replay
  - [brin_xlog_update](brin_xlog_update.md): Handle tuple update replay
  - [brin_xlog_samepage_update](brin_xlog_samepage_update.md): Handle same-page update replay
  - [brin_xlog_revmap_extend](brin_xlog_revmap_extend.md): Handle revmap extension replay
  - [brin_xlog_desummarize_page](brin_xlog_desummarize_page.md): Handle desummarization replay
  - elog: Log error messages for unknown operation codes
- Called from (representative examples):
  - WAL recovery system: Invoked by PostgreSQL's recovery manager during crash recovery

## Notes and Other Information
- This is a public function that serves as the entry point for all BRIN WAL replay operations
- Uses XLOG_BRIN_OPMASK to extract the operation type from the WAL record info field
- Includes error handling for unknown operation codes, triggering a PANIC to halt recovery
- Critical component of PostgreSQL's crash recovery mechanism for maintaining BRIN index consistency
- Each delegated function handles the specific details of replaying its corresponding operation type
- Part of the broader WAL replay infrastructure that ensures database consistency after crashes or restarts