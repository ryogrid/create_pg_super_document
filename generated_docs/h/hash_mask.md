# hash_mask

## Location
src/backend/access/hash/hash_xlog.c: 1121 - 1158

## Overview
This function masks (removes) non-essential data from hash index pages before performing consistency checks during WAL replay, ensuring that transient page state doesn't cause false consistency check failures.

## Definition
```c
void hash_mask(char *pagedata, BlockNumber blkno)
```

## Detailed Description
The hash_mask function is used by PostgreSQL's WAL consistency checking mechanism to mask out page data that may legitimately differ between the primary and standby servers, or between the original page and its replayed version. This function ensures that consistency checks focus only on the essential data that should be identical after proper WAL replay.

The function performs different masking operations based on the page type:

1. **All pages**: Masks LSN, checksum, hint bits, and unused space - these can legitimately differ without indicating corruption
2. **Unused pages**: Masks all content since unused pages don't need to maintain specific data
3. **Bucket and overflow pages**: Masks line pointer flags (LP_FLAGS) which can be modified without WAL logging
4. **All pages**: Masks the LH_PAGE_HAS_DEAD_TUPLES hint bit which may not be logged consistently

This masking is essential for hash indexes because certain optimizations allow some page modifications to occur without generating WAL records, particularly for performance-critical operations like tuple marking during index scans.

## Parameters / Member Variables
- `pagedata`: char pointer to the raw page data to be masked
- `blkno`: BlockNumber of the page being masked (for identification purposes)

## Dependencies
- Functions called/Symbols referenced:
  - [mask_page_lsn_and_checksum](../m/mask_page_lsn_and_checksum.md)
  - [mask_page_hint_bits](../m/mask_page_hint_bits.md)
  - [mask_unused_space](../m/mask_unused_space.md)
  - [mask_page_content](../m/mask_page_content.md)
  - [mask_lp_flags](../m/mask_lp_flags.md)
  - HashPageGetOpaque
- Types referenced:
  - HashPageOpaque
  - Page
- Constants referenced:
  - LH_PAGE_TYPE
  - LH_UNUSED_PAGE
  - LH_BUCKET_PAGE
  - LH_OVERFLOW_PAGE
  - LH_PAGE_HAS_DEAD_TUPLES
- Called from:
  - PostgreSQL WAL consistency checking system (indirectly referenced from SizeOfHashVacuumOnePage)

## Notes and Other Information
- This function is part of PostgreSQL's WAL consistency checking infrastructure
- The masking operations ensure that consistency checks don't fail due to legitimate differences in non-essential page data
- Hash indexes have specific optimizations that allow certain operations to avoid WAL logging for performance reasons
- The LH_PAGE_HAS_DEAD_TUPLES hint bit masking is particularly important because tuple killing operations (_hash_kill_items) may not always generate WAL records
- Line pointer flags can be modified during index scans (hashgettuple) without WAL logging
- Different page types (unused, bucket, overflow) require different masking strategies
- The function is essential for enabling WAL consistency checks on hash indexes without false positives