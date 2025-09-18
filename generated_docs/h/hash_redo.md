# hash_redo

## Location
src/backend/access/hash/hash_xlog.c: 1067 - 1120

## Overview
This is the main WAL replay dispatcher function for hash index operations during PostgreSQL crash recovery, routing different hash-specific WAL record types to their corresponding replay handlers.

## Definition
```c
void hash_redo(XLogReaderState *record)
```

## Detailed Description
The hash_redo function serves as the central dispatch point for all hash index WAL replay operations during PostgreSQL crash recovery. When the recovery process encounters a WAL record related to hash indexes, this function examines the operation type and calls the appropriate specialized replay handler.

The function extracts the operation type from the WAL record's info field and uses a switch statement to route to the correct handler. Each handler is responsible for replaying a specific type of hash index operation, such as page splits, insertions, deletions, vacuum operations, and metadata updates.

This dispatcher pattern ensures that all hash index operations can be properly replayed during crash recovery, maintaining the integrity and consistency of hash indexes after a database restart.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record to be replayed, including the operation type and associated data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - hash_xlog_init_meta_page
  - hash_xlog_init_bitmap_page
  - hash_xlog_insert
  - hash_xlog_add_ovfl_page
  - hash_xlog_split_allocate_page
  - hash_xlog_split_page
  - hash_xlog_split_complete
  - hash_xlog_move_page_contents
  - hash_xlog_squeeze_page
  - hash_xlog_delete
  - hash_xlog_split_cleanup
  - hash_xlog_update_meta_page
  - hash_xlog_vacuum_one_page
  - elog
- Constants referenced:
  - XLR_INFO_MASK
  - XLOG_HASH_INIT_META_PAGE
  - XLOG_HASH_INIT_BITMAP_PAGE
  - XLOG_HASH_INSERT
  - XLOG_HASH_ADD_OVFL_PAGE
  - XLOG_HASH_SPLIT_ALLOCATE_PAGE
  - XLOG_HASH_SPLIT_PAGE
  - XLOG_HASH_SPLIT_COMPLETE
  - XLOG_HASH_MOVE_PAGE_CONTENTS
  - XLOG_HASH_SQUEEZE_PAGE
  - XLOG_HASH_DELETE
  - XLOG_HASH_SPLIT_CLEANUP
  - XLOG_HASH_UPDATE_META_PAGE
  - XLOG_HASH_VACUUM_ONE_PAGE
  - PANIC
- Called from:
  - PostgreSQL WAL recovery system (indirectly referenced from SizeOfHashVacuumOnePage)

## Notes and Other Information
- This is the main entry point for all hash index WAL replay operations
- The function uses a comprehensive switch statement to handle all supported hash index WAL record types
- Each case delegates to a specialized handler function that knows how to replay that specific operation type
- Unknown operation codes trigger a PANIC to prevent corruption during recovery
- The function is part of PostgreSQL's access method interface for WAL recovery
- All hash-specific WAL operations must be handled through this dispatcher to ensure proper recovery
- The XLR_INFO_MASK is used to extract the operation type from the record's info field