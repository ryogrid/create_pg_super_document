# ginxlogVacuumDataLeafPage

## Location
[src/include/access/ginxlog.h:143-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/ginxlog.h#L143-L146)

## Overview
Structure used for WAL (Write-Ahead Logging) record when vacuuming GIN index data leaf pages through recompression operations.

## Definition
```c
typedef struct ginxlogVacuumDataLeafPage
{
    ginxlogRecompressDataLeaf data;
} ginxlogVacuumDataLeafPage;
```

## Detailed Description
The ginxlogVacuumDataLeafPage structure is used as part of WAL logging when vacuum operations are performed on GIN (Generalized Inverted Index) data leaf pages. This structure is a wrapper around ginxlogRecompressDataLeaf and is used specifically for vacuum operations that recompress the data in leaf pages. The vacuum process may remove dead tuples and reorganize the posting lists on data pages, and this WAL record ensures these operations can be properly replayed during recovery.

## Parameters / Member Variables
- `data`: A ginxlogRecompressDataLeaf structure containing:
  - `nactions`: Number of recompression actions to perform
  - Variable number of action records follow (containing segment numbers, action types, and action-specific data)

## Dependencies
- Functions called/Symbols referenced:
  - ginxlogRecompressDataLeaf
- Called from (representative examples):
  - [ginRedoVacuumDataLeafPage](ginRedoVacuumDataLeafPage.md) (in src/backend/access/gin/ginxlog.c:461,463)
  - [gin_desc](gin_desc.md) (in src/backend/access/rmgrdesc/gindesc.c:156,157)

## Notes and Other Information
- This structure is part of the GIN index WAL logging infrastructure (XLOG_GIN_VACUUM_DATA_LEAF_PAGE operation)
- Used specifically for vacuum operations that recompress data leaf pages
- The underlying ginxlogRecompressDataLeaf contains a variable number of action records
- During recovery, ginRedoRecompress processes the data member to replay the recompression operations
- Only applies to leaf pages in the data tree (not entry tree)
- Defined in src/include/access/ginxlog.h:143-146