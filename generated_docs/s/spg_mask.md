# spg_mask

## Location
[src/backend/access/spgist/spgxlog.c:994-1009](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgxlog.c#L994-L1009)

## Overview
Masks (ignores) specific fields in an SP-GiST page before performing consistency checks during WAL replay operations.

## Definition

```c
void
spg_mask(char *pagedata, BlockNumber blkno)
```
## Detailed Description
 is a utility function used during SP-GiST WAL replay to prepare pages for consistency checking. It masks out fields that are expected to differ between the original and replayed versions of a page, allowing the consistency check to focus on the essential data content. The function masks the page LSN (Log Sequence Number) and checksum, page hint bits, and unused space within the page. The unused space masking is conditional - it only occurs if the page's  field appears to be set correctly (i.e., greater than or equal to the size of the page header), ensuring that the masking operation is safe and won't interfere with valid page data.

## Parameters / Member Variables
- `*pagedata`: Character pointer to the raw page data to be masked
- `blkno`: Block number of the page (parameter present but not used in current implementation)
## Dependencies
- Functions called/Symbols referenced:
  - Page (page data type cast)
  - PageHeader (page header data type cast)
  - [mask_page_lsn_and_checksum](../m/mask_page_lsn_and_checksum.md) (masks LSN and checksum fields)
  - [mask_page_hint_bits](../m/mask_page_hint_bits.md) (masks hint bit fields)
  - [mask_unused_space](../m/mask_unused_space.md) (masks unused portions of the page)
  - SizeOfPageHeaderData (constant defining size of page header)
- Called from (representative examples):
  - SizeOfSpgxlogVacuumRedirect (referenced in spgxlog.h)

## Notes and Other Information
- This function is part of the consistency checking infrastructure for SP-GiST WAL replay
- Masking operations help avoid false positives during consistency checks by ignoring fields that legitimately differ between master and replica
- The  parameter is provided for interface compatibility but is not currently used in the implementation
- Located in src/backend/access/spgist/spgxlog.c:994-1009
- The conditional unused space masking prevents potential corruption of valid page data by ensuring pd_lower is reasonable before proceeding

## Simplified Source

```c
void spg_mask(char *pagedata, BlockNumber blkno) {
    Page page = (Page) pagedata;
    PageHeader pagehdr = (PageHeader) page;

    // Mask fields that are expected to differ between original and replayed pages
    mask_page_lsn_and_checksum(page);
    mask_page_hint_bits(page);

    // Mask unused space only if page header looks valid
    if (pagehdr->pd_lower >= SizeOfPageHeaderData) {
        mask_unused_space(page);
    }
}
```