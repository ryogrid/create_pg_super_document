# brin_xlog_update

## Location
[src/backend/access/brin/brin_xlog.c:135-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_xlog.c#L135-L169)

## Overview
A static function that handles WAL replay for BRIN index update operations during crash recovery by removing old tuples and inserting new ones.

## Definition
```c
static void brin_xlog_update(XLogReaderState *record)
```

## Detailed Description
This function performs BRIN index update replay operations during crash recovery. Updates in BRIN indexes are implemented as a combination of deletion and insertion: first, the old tuple is removed from its original location using PageIndexTupleDeleteNoCompact, then the new tuple is inserted and the revmap is updated by delegating to brin_xlog_insert_update. This approach ensures that the updated tuple can be placed in the optimal location while maintaining index consistency.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the BRIN update operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extracts the data portion from the WAL record
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md): Reads buffer for redo operations
  - [PageIndexTupleDeleteNoCompact](../P/PageIndexTupleDeleteNoCompact.md): Removes the old tuple without compacting the page
  - [brin_xlog_insert_update](brin_xlog_insert_update.md): Inserts the new tuple and updates revmap
  - [xl_brin_update](../x/xl_brin_update.md): Structure containing BRIN update parameters including old offset and insert data
- Called from (representative examples):
  - [brin_redo](brin_redo.md): Main BRIN WAL replay dispatcher function

## Notes and Other Information
- This is a static function only accessible within the brin_xlog.c file
- Implements the update operation as delete-then-insert for flexibility in tuple placement
- Uses PageIndexTupleDeleteNoCompact to avoid unnecessary page reorganization during the delete phase
- Leverages the existing brin_xlog_insert_update function for the insertion portion
- Part of PostgreSQL's crash recovery mechanism for BRIN indexes
- Located at src/backend/access/brin/brin_xlog.c:135-169
- The function handles three buffers: two for the insertion (regular page and revmap) and one for the deletion (old tuple location)