# GinBtreeDataLeafInsertData

## Location
src/include/access/gin_private.h: 198 - 241

## Overview
GinBtreeDataLeafInsertData represents one or many item pointers (TIDs) to be inserted into a GIN data (posting tree) leaf page, with tracking for batch processing operations.

## Definition
```c
typedef struct
{
    ItemPointerData *items;
    uint32          nitem;
    uint32          curitem;
} GinBtreeDataLeafInsertData;
```

## Detailed Description
GinBtreeDataLeafInsertData is a structure used for bulk insertion operations into GIN data leaf pages. It maintains an array of item pointers (tuple identifiers) along with count information and a current position tracker. This structure enables efficient batch processing of multiple TID insertions into posting tree leaf pages, allowing the GIN access method to optimize insertion operations by processing multiple items together rather than individually.

## Parameters / Member Variables
- `items`: Pointer to an array of ItemPointerData structures containing the item pointers (TIDs) to be inserted
- `nitem`: Total number of items in the items array
- `curitem`: Current position/index within the items array, used for tracking progress during batch operations

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerData](../I/ItemPointerData.md) (PostgreSQL item pointer type)
- Called from (representative examples):
  - [dataBeginPlaceToPageLeaf](../d/dataBeginPlaceToPageLeaf.md)
  - [ginInsertItemPointers](../g/ginInsertItemPointers.md)

## Notes and Other Information
This structure is specifically designed for GIN posting tree leaf page operations and supports batch insertion scenarios where multiple TIDs need to be inserted efficiently. The curitem field allows for resumable operations and tracking of progress through large batches of items. The structure is part of the GIN access method's optimization strategy for handling bulk data insertion into posting trees, which are used to store the actual tuple identifiers for indexed values.