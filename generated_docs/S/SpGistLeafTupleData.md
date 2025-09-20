# SpGistLeafTupleData

## Location
[src/include/access/spgist_private.h:383-392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist_private.h#L383-L392)

## Overview
SpGistLeafTupleData represents the structure of leaf tuples in SP-GiST indexes, containing leaf data values, heap tuple references, and optional included columns.

## Definition

```c
typedef struct SpGistLeafTupleData
{
	unsigned int tupstate:2,	/* LIVE/REDIRECT/DEAD/PLACEHOLDER */
				size:30;		/* large enough for any palloc'able value */
	uint16		t_info;			/* nextOffset, which links to the next tuple
								 * in chain, plus two flag bits */
	ItemPointerData heapPtr;	/* TID of represented heap tuple */
	/* nulls bitmap follows if the flag bit for it is set */
	/* leaf datum, then any included datums, follows on a MAXALIGN boundary */
} SpGistLeafTupleData;
```
## Detailed Description
SpGistLeafTupleData defines the on-disk format for leaf tuples in SP-GiST indexes. These tuples store the actual indexed values (or derived representations like suffixes) along with heap tuple identifiers and optional included columns. The structure supports various tuple states and implements chaining for tuples belonging to the same parent node. The design optimizes space usage through bit packing and conditional nulls bitmap inclusion, while maintaining alignment requirements for efficient access.

## Parameters / Member Variables
- : 2-bit field indicating tuple state (LIVE, REDIRECT, DEAD, or PLACEHOLDER)
- : 30-bit field storing tuple size, must be MAXALIGN multiple and at least SGDTSIZE
- : 16-bit field containing nextOffset (14 bits) for tuple chaining plus flag bits including has-nulls-bitmap
- : ItemPointerData structure containing the TID of the corresponding heap tuple
- Nulls bitmap: Optional bitmap present when included columns exist and any datums are NULL
- Leaf datum: The actual indexed value or derived representation (suffix/delta)
- Included datums: Optional additional column values stored without modification

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerData](../I/ItemPointerData.md) (implicitly referenced)
- Called from (representative examples):
  - [spgFormLeafTuple](../s/spgFormLeafTuple.md)
  - [spgDeformLeafTuple](../s/spgDeformLeafTuple.md)
  - [spgRedoAddLeaf](../s/spgRedoAddLeaf.md)
  - [spgRedoMoveLeafs](../s/spgRedoMoveLeafs.md)
  - [spgRedoPickSplit](../s/spgRedoPickSplit.md)
  - SpGistLeafTuple
  - SGLTHDRSZ

## Notes and Other Information
- Leaf datum can be the same as indexed value or a suffix/delta requiring prefix path knowledge for reconstruction
- Nulls bitmap size is INDEX_MAX_KEYS bits when present, regardless of actual attribute count
- nextOffset links tuples belonging to the same parent node, except on root leaf pages where it's always 0
- Size field is wider than needed for on-disk storage to allow formation of oversized tuples during processing
- The structure supports backwards compatibility for null leaf datums without included columns
- Alignment requirements ensure optimal memory access patterns and efficient space utilization