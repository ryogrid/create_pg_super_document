# hash_mask

## Location
[src/backend/access/hash/hash_xlog.c:1121-1158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L1121-L1158)

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
- [Hash](../H/Hash.md) indexes have specific optimizations that allow certain operations to avoid WAL logging for performance reasons
- The LH_PAGE_HAS_DEAD_TUPLES hint bit masking is particularly important because tuple killing operations (_hash_kill_items) may not always generate WAL records
- Line pointer flags can be modified during index scans (hashgettuple) without WAL logging
- Different page types (unused, bucket, overflow) require different masking strategies
- The function is essential for enabling WAL consistency checks on hash indexes without false positives

## Simplified Source

```c
void hash_mask(char *pagedata, BlockNumber blkno) {
    Page page = (Page) pagedata;
    HashPageOpaque opaque;
    int pagetype;

    // Mask standard page elements that can legitimately differ
    mask_page_lsn_and_checksum(page);
    mask_page_hint_bits(page);
    mask_unused_space(page);

    opaque = HashPageGetOpaque(page);
    pagetype = opaque->hasho_flag & LH_PAGE_TYPE;

    if (pagetype == LH_UNUSED_PAGE) {
        // Unused pages can have arbitrary content
        mask_page_content(page);
    }
    else if (pagetype == LH_BUCKET_PAGE || pagetype == LH_OVERFLOW_PAGE) {
        // Line pointer flags can be modified without WAL logging
        // during operations like hashgettuple() and _hash_kill_items()
        mask_lp_flags(page);
    }

    // The dead tuples hint bit may not be consistently logged
    opaque->hasho_flag &= ~LH_PAGE_HAS_DEAD_TUPLES;
}
```