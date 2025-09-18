# spgxlogAddNode

## Location
[src/include/access/spgxlog.h:99-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgxlog.h#L99-L135)

## Overview
The spgxlogAddNode structure contains WAL record data for SP-GiST operations that add or update inner (non-leaf) nodes in the index tree structure.

## Definition
```c
typedef struct spgxlogAddNode
{
    /*
     * Offset of the original inner tuple, in the original page (on backup
     * block 0).
     */
    OffsetNumber offnum;
    
    /*
     * Offset of the new tuple, on the new page (on backup block 1). Invalid,
     * if we overwrote the old tuple in the original page).
     */
    OffsetNumber offnumNew;
    bool         newPage;        /* init new page? */
    
    /*----
     * Where is the parent downlink? parentBlk indicates which page its on,