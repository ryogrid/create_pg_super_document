# brin_xlog_samepage_update

## Location
[src/backend/access/brin/brin_xlog.c:170-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_xlog.c#L170-L207)

## Overview
A static function that handles WAL replay for in-place BRIN tuple updates that occur within the same page during crash recovery.

## Definition
```c
static void brin_xlog_samepage_update(XLogReaderState *record)
```

## Detailed Description
This function performs optimized BRIN index update replay operations when the updated tuple can fit in the same page location as the original tuple. Unlike regular updates that perform delete-then-insert operations, same-page updates use PageIndexTupleOverwrite to replace the existing tuple in-place, which is more efficient as it avoids the need to update the revmap or handle page reorganization. This optimization is possible when the new tuple data can fit within the space constraints of the original tuple's location.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the BRIN same-page update operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extracts the data portion from the WAL record
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md): Reads buffer for redo operations
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md): Extracts block data from WAL record
  - [PageIndexTupleOverwrite](../P/PageIndexTupleOverwrite.md): Overwrites existing tuple in-place
  - [xl_brin_samepage_update](../x/xl_brin_samepage_update.md): Structure containing same-page update parameters
  - [BrinTuple](../B/BrinTuple.md): BRIN tuple structure
- Called from (representative examples):
  - [brin_redo](brin_redo.md): Main BRIN WAL replay dispatcher function

## Notes and Other Information
- This is a static function only accessible within the brin_xlog.c file
- Provides an optimized path for updates that don't require tuple relocation
- Uses PageIndexTupleOverwrite for efficient in-place replacement
- No revmap updates are needed since the tuple location doesn't change
- Includes error handling with PANIC if the overwrite operation fails
- Part of PostgreSQL's crash recovery mechanism for BRIN indexes
- Located at src/backend/access/brin/brin_xlog.c:170-207
- No FSM (Free Space Map) updates are performed as noted in the comment
- More efficient than regular updates for cases where tuple size constraints allow in-place replacement