# gist_mask

## Location
[src/backend/access/gist/gistxlog.c:453-494](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistxlog.c#L453-L494)

## Overview
Masks a GiST (Generalized Search Tree) page before running consistency checks by setting various page elements to standardized values to ignore differences that don't affect logical consistency.

## Definition

```c
void
gist_mask(char *pagedata, BlockNumber blkno)
```
## Detailed Description
The  function is part of PostgreSQL's WAL (Write-Ahead Logging) consistency checking mechanism for GiST indexes. It modifies a page image to mask out fields that can legitimately differ between the primary and standby servers without indicating a real consistency problem. This function is called during WAL replay consistency checks to normalize page contents before comparison.

The function performs several masking operations:
- Masks LSN (Log Sequence Number) and checksum values that naturally differ between servers
- Masks page hint bits and unused space that may vary
- Sets NSN (Next Split Number) to a standard marker value
- Sets the F_FOLLOW_RIGHT flag to handle split-related timing differences
- For leaf pages, masks line pointer flags that can be modified without WAL logging
- Clears the "has garbage" flag since it's never set during redo operations

## Parameters / Member Variables
- : Pointer to the raw page data to be masked (cast to Page internally)
- : Block number of the page being masked (currently unused in implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [mask_page_lsn_and_checksum](../m/mask_page_lsn_and_checksum.md)
  - [mask_page_hint_bits](../m/mask_page_hint_bits.md)
  - [mask_unused_space](../m/mask_unused_space.md)
  - GistPageSetNSN
  - GistMarkFollowRight
  - GistPageIsLeaf
  - [mask_lp_flags](../m/mask_lp_flags.md)
  - GistClearPageHasGarbage
  - MASK_MARKER
- Called from (representative examples):
  - Used in WAL consistency checking framework (referenced in gistxlog.h)

## Notes and Other Information
- This function is specifically designed for WAL consistency checking and should not be used in normal GiST operations
- The masking is necessary because certain page modifications can occur without generating WAL records, leading to legitimate differences between primary and standby servers
- The F_FOLLOW_RIGHT flag masking addresses timing issues during page splits where the flag may be set at different times on primary vs standby
- Line pointer flag masking in leaf pages accounts for the gistkillitems() function which can modify these flags without WAL logging