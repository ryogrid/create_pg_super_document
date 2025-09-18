# GinBtreeEntryInsertData

## Location
src/include/access/gin_private.h: 187 - 197

## Overview
GinBtreeEntryInsertData is a structure that represents a tuple to be inserted into a GIN entry tree, along with metadata indicating whether an existing tuple should be deleted.

## Definition
```c
typedef struct
{
    IndexTuple  entry;      /* tuple to insert */
    bool        isDelete;   /* delete old tuple at same offset? */
} GinBtreeEntryInsertData;
```

## Detailed Description
GinBtreeEntryInsertData encapsulates the information needed to perform an insertion operation on a GIN entry tree. The structure contains both the actual index tuple to be inserted and a flag that indicates whether the operation should also delete an existing tuple at the same location. This design supports both pure insertion operations and replacement operations where an existing entry needs to be updated or replaced with new content.

## Parameters / Member Variables
- `entry`: IndexTuple containing the actual tuple data to be inserted into the GIN entry tree
- `isDelete`: Boolean flag indicating whether an existing tuple at the same offset should be deleted as part of this insertion operation

## Dependencies
- Functions called/Symbols referenced:
  - IndexTuple (PostgreSQL index tuple type)
- Called from (representative examples):
  - entryIsEnoughSpace
  - entryPreparePage
  - entryBeginPlaceToPage
  - entryExecPlaceToPage
  - entrySplitPage
  - entryPrepareDownlink
  - ginEntryInsert

## Notes and Other Information
This structure is specifically designed for GIN entry tree operations and is part of the GIN access method's internal API. The isDelete flag enables atomic replace operations where old data is removed and new data is inserted in a single logical operation. The structure is used throughout the GIN entry page management functions to coordinate insertion and deletion operations efficiently.