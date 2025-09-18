# spgxlogAddLeaf

## Location
src/include/access/spgxlog.h: 46 - 57

## Overview
The spgxlogAddLeaf structure contains WAL record data for SP-GiST operations that add a new leaf tuple to the index.

## Definition
```c
typedef struct spgxlogAddLeaf
{
    bool          newPage;        /* init dest page? */
    bool          storesNulls;    /* page is in the nulls tree? */
    OffsetNumber  offnumLeaf;     /* offset where leaf tuple gets placed */
    OffsetNumber  offnumHeadLeaf; /* offset of head tuple in chain, if any */
    
    OffsetNumber  offnumParent;   /* where the parent downlink is, if any */
    uint16        nodeI;
    
    /* new leaf tuple follows (unaligned!) */
} spgxlogAddLeaf;
```

## Detailed Description
The spgxlogAddLeaf structure is used in SP-GiST WAL records to capture all the information needed to replay the operation of adding a new leaf tuple to the index during recovery. This structure handles various scenarios including adding to new pages, managing tuple chains, and maintaining parent-child relationships in the SP-GiST tree structure. The structure uses backup blocks 0 and 1 for the destination page and parent page respectively.

## Parameters / Member Variables
- `newPage`: Boolean flag indicating whether the destination page needs to be initialized
- `storesNulls`: Boolean flag indicating whether this page is part of the nulls tree (special handling for NULL values)
- `offnumLeaf`: Offset number where the new leaf tuple will be placed on the page
- `offnumHeadLeaf`: Offset number of the head tuple in the chain, used when the new leaf is part of a tuple chain
- `offnumParent`: Offset number indicating where the parent downlink is located, if there is a parent relationship
- `nodeI`: 16-bit identifier for the node
- Note: The actual new leaf tuple data follows this structure in the WAL record (unaligned)

## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber (PostgreSQL offset type)
  - uint16 (standard integer type)
- Called from (representative examples):
  - spg_desc (in spgdesc.c:29)
  - addLeafTuple (in spgdoinsert.c:206)
  - spgRedoAddLeaf (in spgxlog.c:78, 85)

## Notes and Other Information
- The structure is designed for WAL replay operations and contains minimal information necessary for reconstructing the add leaf operation
- Backup blocks are used to store page images: Block 0 for destination page, Block 1 for parent page
- The actual leaf tuple data is stored immediately after this structure in the WAL record in an unaligned format
- Special handling is provided for NULL values through the storesNulls flag
- The structure supports both simple leaf additions and complex chain operations through the head leaf offset