# spgxlogMoveLeafs

## Location
src/include/access/spgxlog.h: 64 - 89

## Overview
The spgxlogMoveLeafs structure contains WAL record data for SP-GiST operations that move multiple leaf tuples from a source page to a destination page.

## Definition
```c
typedef struct spgxlogMoveLeafs
{
    uint16        nMoves;         /* number of tuples moved from source page */
    bool          newPage;        /* init dest page? */
    bool          replaceDead;    /* are we replacing a DEAD source tuple? */
    bool          storesNulls;    /* pages are in the nulls tree? */
    
    /* where the parent downlink is */
    OffsetNumber  offnumParent;
    uint16        nodeI;
    
    spgxlogState  stateSrc;
    
    /*----------
     * data follows:
     *      array of deleted tuple numbers, length nMoves
     *      array of inserted tuple numbers, length nMoves + 1 or 1
     *      list of leaf tuples, length nMoves + 1 or 1 (unaligned!)
     *
     * Note: if replaceDead is true then there is only one inserted tuple
     * number and only one leaf tuple in the data, because we are not copying
     * the dead tuple from the source
     *----------
     */
    OffsetNumber  offsets[FLEXIBLE_ARRAY_MEMBER];
} spgxlogMoveLeafs;
```

## Detailed Description
The spgxlogMoveLeafs structure is used in SP-GiST WAL records to capture complex operations that involve moving multiple leaf tuples between pages. This operation is typically part of page splitting or reorganization within the SP-GiST index structure. The structure supports both normal move operations and special cases where dead tuples are being replaced. The structure uses three backup blocks: source leaf page (Block 0), destination leaf page (Block 1), and parent page (Block 2).

## Parameters / Member Variables
- `nMoves`: Number of tuples being moved from the source page to the destination page
- `newPage`: Boolean flag indicating whether the destination page needs to be initialized
- `replaceDead`: Boolean flag indicating whether this operation is replacing a DEAD source tuple (affects data layout)
- `storesNulls`: Boolean flag indicating whether the pages are part of the nulls tree (special handling for NULL values)
- `offnumParent`: Offset number indicating where the parent downlink is located
- `nodeI`: 16-bit identifier for the node
- `stateSrc`: Embedded spgxlogState structure containing additional state information for the source
- `offsets`: Flexible array member that begins the variable-length data section containing offset arrays and tuple data

## Dependencies
- Functions called/Symbols referenced:
  - spgxlogState (embedded state structure)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member)
  - OffsetNumber (PostgreSQL offset type)
  - uint16 (standard integer type)
- Called from (representative examples):
  - spg_desc (in spgdesc.c:42)
  - moveLeafs (in spgdoinsert.c:403)
  - spgRedoMoveLeafs (in spgxlog.c:175)
  - SizeOfSpgxlogMoveLeafs (in spgxlog.h:91)

## Notes and Other Information
- This is a complex WAL record structure that handles bulk tuple movement operations
- The variable-length data section contains three arrays: deleted tuple numbers, inserted tuple numbers, and the actual leaf tuple data
- When replaceDead is true, the data layout is simplified to contain only one inserted tuple and one leaf tuple
- Uses three backup blocks to store complete page images for source, destination, and parent pages
- The tuple data is stored unaligned to minimize WAL record size
- Special handling is provided for NULL values through the storesNulls flag
- The structure is designed to support efficient page reorganization and splitting operations in SP-GiST indexes