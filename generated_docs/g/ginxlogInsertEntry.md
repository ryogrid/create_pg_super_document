# ginxlogInsertEntry

## Location
[src/include/access/ginxlog.h:62-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/ginxlog.h#L62-L69)

## Overview
ginxlogInsertEntry is a WAL record structure used specifically for logging entry insertion operations in PostgreSQL's GIN index entry trees, containing the data needed to replay entry insertions and deletions during crash recovery.

## Definition
```c
typedef struct
{
    OffsetNumber offset;
    bool         isDelete;
    IndexTupleData tuple;       /* variable length */
} ginxlogInsertEntry;
```

## Detailed Description
The ginxlogInsertEntry structure is used to log insertion and deletion operations on GIN index entry pages. This structure follows a ginxlogInsert header in WAL records and contains the specific information needed to replay entry-level operations during crash recovery. The structure supports both insertion and deletion operations through the isDelete flag, and includes a variable-length index tuple that contains the actual data being inserted or the key for deletion.

This structure is specifically used for entry tree operations (as opposed to posting tree operations) and is part of the variable-length data that follows the ginxlogInsert header in XLOG_GIN_INSERT WAL records.

## Parameters / Member Variables
- `offset`: OffsetNumber indicating the position on the page where the operation should be performed
- `isDelete`: Boolean flag indicating whether this is a deletion operation (true) or insertion operation (false)  
- `tuple`: Variable-length IndexTupleData containing the index tuple data being inserted or the key being deleted

## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber (for page offset positioning)
  - [IndexTupleData](../I/IndexTupleData.md) (for tuple storage)

- Called from (representative examples):
  - [entryExecPlaceToPage](../e/entryExecPlaceToPage.md) (src/backend/access/gin/ginentrypage.c:582, 589)
  - [ginRedoInsertEntry](ginRedoInsertEntry.md) (src/backend/access/gin/ginxlog.c:74)
  - [gin_desc](gin_desc.md) (src/backend/access/rmgrdesc/gindesc.c:115)

## Notes and Other Information
- This structure has only 16-bit alignment when appended to a ginxlogInsert struct
- The tuple field is variable length, making the overall structure size variable
- Used specifically for GIN entry tree operations (not posting tree operations)
- Critical for maintaining consistency during WAL replay of GIN index modifications
- The same structure handles both insertions and deletions, differentiated by the isDelete flag