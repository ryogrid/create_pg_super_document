# xl_brin_revmap_extend

## Location
[src/include/access/brin_xlog.h:115-122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/brin_xlog.h#L115-L122)

## Overview
A WAL record structure for logging the extension of a BRIN index's revmap (reverse mapping) when additional space is needed to track new heap block ranges.

## Definition

```c
typedef struct xl_brin_revmap_extend
{
	/*
	 * XXX: This is actually redundant - the block number is stored as part of
	 * backup block 1.
	 */
	BlockNumber targetBlk;
} xl_brin_revmap_extend;
```
## Detailed Description
The  structure is used to log the extension of a BRIN index's revmap (reverse mapping) structure. The revmap is a critical component of BRIN indexes that maintains the mapping between heap block ranges and their corresponding index tuple locations. When a BRIN index grows and needs to track additional heap blocks, the revmap must be extended to accommodate these new mappings.

This WAL record works with two backup blocks: backup block 0 contains the metapage (which tracks the overall structure of the BRIN index), and backup block 1 contains the new revmap page being added. The operation updates the metapage to reflect the new revmap page and initializes the new page for use.

## Parameters / Member Variables
- `targetBlk`: The block number of the new revmap page being added (note: the code comments indicate this field is redundant since the block number is also stored as part of backup block 1)
## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (type)
- Called from (representative examples):
  - [revmap_physical_extend](../r/revmap_physical_extend.md) (in src/backend/access/brin/brin_revmap.c:624)
  - [brin_xlog_revmap_extend](../b/brin_xlog_revmap_extend.md) (in src/backend/access/brin/brin_xlog.c:211, 218)
  - [brin_desc](../b/brin_desc.md) (in src/backend/access/rmgrdesc/brindesc.c:60)
  - SizeOfBrinRevmapExtend (macro in src/include/access/brin_xlog.h:124)

## Notes and Other Information
- The revmap extension is necessary when a BRIN index needs to track heap blocks beyond its current capacity
- Uses two backup blocks: metapage (block 0) and the new revmap page (block 1)
- The  field is noted as redundant in the code comments, as the block number is already stored in the backup block metadata
- The  macro calculates the size of this structure for WAL operations
- This operation is relatively infrequent compared to tuple insertions and updates, occurring only when the revmap capacity is exceeded
- The revmap extension maintains the overall integrity of the BRIN index by ensuring all heap block ranges can be properly mapped to their summary tuples