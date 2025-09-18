# ginxlogSplit

## Location
src/include/access/ginxlog.h: 111 - 119

## Overview
Structure used for WAL (Write-Ahead Logging) record when splitting pages in a GIN index B-tree structure.

## Definition
```c
typedef struct ginxlogSplit
{
    RelFileLocator locator;
    BlockNumber rrlink;         /* right link, or root's blocknumber if root split */
    BlockNumber leftChildBlkno; /* valid on a non-leaf split */
    BlockNumber rightChildBlkno;
    uint16      flags;          /* see below */
} ginxlogSplit;
```

## Detailed Description
The ginxlogSplit structure is used as part of WAL logging when splitting pages in GIN (Generalized Inverted Index) B-tree structures. This operation is fundamental to maintaining GIN index structure when pages become full and need to be split. The structure contains metadata about the split operation, including references to the child blocks (for internal page splits), the right link information, and flags indicating the type of split operation (data vs entry tree, leaf vs internal, root split).

## Parameters / Member Variables
- `locator`: File locator identifying the relation being modified
- `rrlink`: Right link pointer, or root's block number if this is a root split operation
- `leftChildBlkno`: Block number of the left child page (valid only for non-leaf splits)
- `rightChildBlkno`: Block number of the right child page
- `flags`: Bit flags indicating split characteristics (GIN_INSERT_ISDATA, GIN_INSERT_ISLEAF, GIN_SPLIT_ROOT)

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](../R/RelFileLocator.md)
  - BlockNumber
- Called from (representative examples):
  - [ginPlaceToPage](ginPlaceToPage.md) (in src/backend/access/gin/ginbtree.c:460,620)
  - [ginRedoSplit](ginRedoSplit.md) (in src/backend/access/gin/ginxlog.c:404)
  - [gin_desc](gin_desc.md) (in src/backend/access/rmgrdesc/gindesc.c:133,136)

## Notes and Other Information
- This structure is part of the GIN index WAL logging infrastructure (XLOG_GIN_SPLIT operation)
- Flags can be combined: GIN_INSERT_ISDATA (0x01), GIN_INSERT_ISLEAF (0x02), GIN_SPLIT_ROOT (0x04)
- Used both during normal B-tree split operations and during WAL replay
- The structure is followed by full page images of the affected pages in the WAL record
- Defined in src/include/access/ginxlog.h:111-119