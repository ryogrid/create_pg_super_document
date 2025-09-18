# spgxlogSplitTuple

## Location
src/include/access/spgxlog.h: 141 - 156

## Overview
The spgxlogSplitTuple structure contains WAL record data for SP-GiST operations that split an inner tuple into prefix and postfix components.

## Definition
```c
typedef struct spgxlogSplitTuple
{
    /* where the prefix tuple goes */
    OffsetNumber offnumPrefix;
    
    /* where the postfix tuple goes */
    OffsetNumber offnumPostfix;
    bool         newPage;        /* need to init that page? */
    bool         postfixBlkSame; /* was postfix tuple put on same page as
                                  * prefix? */
    
    /*
     * new prefix inner tuple follows, then new postfix inner tuple (both are
     * unaligned!)
     */
} spgxlogSplitTuple;
```

## Detailed Description
The spgxlogSplitTuple structure is used in SP-GiST WAL records to capture operations that split an existing inner tuple into two separate tuples: a prefix tuple and a postfix tuple. This operation is fundamental to SP-GiST index maintenance and typically occurs when the index needs to refine its tree structure to accommodate new data patterns. The split operation may place both tuples on the same page or distribute them across different pages, with the structure providing flexibility to handle both scenarios. The structure uses up to two backup blocks: one for the prefix tuple location (Block 0) and optionally one for the postfix tuple if it goes to a different page (Block 1).

## Parameters / Member Variables
- `offnumPrefix`: Offset number where the prefix tuple will be placed
- `offnumPostfix`: Offset number where the postfix tuple will be placed
- `newPage`: Boolean flag indicating whether the destination page for one of the tuples needs to be initialized
- `postfixBlkSame`: Boolean flag indicating whether the postfix tuple was placed on the same page as the prefix tuple (affects backup block usage)
- Note: The actual prefix and postfix inner tuple data follows this structure in the WAL record (both unaligned)

## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber (PostgreSQL offset type)
  - [bool](../b/bool.md) (boolean type)
- Called from (representative examples):
  - [spg_desc](spg_desc.md) (in spgdesc.c:72)
  - [spgSplitNodeAction](spgSplitNodeAction.md) (in spgdoinsert.c:1726)
  - [spgRedoSplitTuple](spgRedoSplitTuple.md) (in spgxlog.c:455, 464)

## Notes and Other Information
- This structure represents a key operation in SP-GiST index maintenance where complex inner tuples are split for better tree organization
- The split operation creates two separate inner tuples from one original tuple, allowing for more refined tree navigation
- Uses conditional backup blocks: always uses Block 0 for prefix location, and uses Block 1 only if postfix goes to a different page
- Both prefix and postfix tuple data are stored immediately after this structure in unaligned format to minimize WAL record size
- The postfixBlkSame flag optimizes storage by indicating when both tuples share the same page
- This operation is critical for maintaining SP-GiST index efficiency and search performance
- The split typically occurs when the original tuple prefix needs to be shortened and the remaining portion becomes a separate postfix tuple