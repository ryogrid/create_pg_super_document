# brinRevmapExtend

## Location
src/backend/access/brin/brin_revmap.c: 112 - 133

## Overview
Extends the BRIN revmap to ensure coverage for a given heap block number, allocating new revmap pages if necessary.

## Definition
```c
void brinRevmapExtend(BrinRevmap *revmap, BlockNumber heapBlk)
```

## Detailed Description
This function ensures that the BRIN revmap has sufficient pages to cover the specified heap block number. The revmap maintains a mapping from heap block ranges to index tuple locations, and as the heap grows, the revmap must be extended accordingly. The function delegates the core extension logic to revmap_extend_and_get_blkno, which calculates the required revmap block number and performs physical extension if needed.

The extension process involves:
1. Calculating the required revmap block number for the given heap block
2. Extending the revmap's physical storage if the required block exceeds current capacity
3. Validating that the resulting block number is within expected bounds
4. Ensuring the revmap structure is updated to reflect the new extent

The function includes assertion checks to verify that the returned block number is valid, not the metadata page, and within the expected range of revmap pages.

## Parameters / Member Variables
- `revmap`: The BrinRevmap access object to extend
- `heapBlk`: The heap block number that must be covered by the revmap

## Dependencies
- Functions called/Symbols referenced:
  - revmap_extend_and_get_blkno
- Types referenced:
  - BrinRevmap
  - BlockNumber
  - PG_USED_FOR_ASSERTS_ONLY
  - BRIN_METAPAGE_BLKNO
  - InvalidBlockNumber
- Called from:
  - brin_doupdate
  - brin_doinsert

## Notes and Other Information
- The function uses PG_USED_FOR_ASSERTS_ONLY to mark variables only used in assertions
- Extension is performed lazily - pages are only added when needed to cover specific heap blocks
- The function ensures proper bounds checking through assertions in debug builds
- Physical extension involves updating both the revmap structure and the index metadata
- This function is typically called during BRIN tuple insertion and update operations when the heap has grown beyond the current revmap coverage