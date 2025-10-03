# btree_mask

## Location
[src/backend/access/nbtree/nbtxlog.c:1091-1127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L1091-L1127)

## Overview
The btree_mask function masks a btree page before performing consistency checks on it by clearing certain un-logged hint bits and flags that may vary between primary and standby servers.

## Definition

```c
void
btree_mask(char *pagedata, BlockNumber blkno)
```
## Detailed Description
This function is part of PostgreSQL's Write-Ahead Logging (WAL) consistency checking mechanism for btree indexes. It prepares a btree page for comparison by masking (clearing) various flags and fields that can legitimately differ between the primary server and standby servers, even when the pages are logically identical.

The function performs several masking operations:
1. Masks the page LSN (Log Sequence Number) and checksum using standard page masking utilities
2. Masks page hint bits and unused space on the page
3. For leaf pages specifically, masks line pointer flags that can be modified without WAL logging
4. Clears the BTP_HAS_GARBAGE flag, which is an un-logged hint bit
5. Clears the BTP_SPLIT_END flag and resets the cycle ID, as these may not be set consistently during WAL replay

## Parameters / Member Variables
- `*pagedata`: Pointer to the raw page data that needs to be masked for consistency checking
- `blkno`: Block number of the page being masked (currently unused in the implementation)
## Dependencies
- Functions called/Symbols referenced:
  - BTPageOpaque (btree page opaque structure)
  - [mask_page_lsn_and_checksum](../m/mask_page_lsn_and_checksum.md) (masks LSN and checksum)
  - [mask_page_hint_bits](../m/mask_page_hint_bits.md) (masks hint bits)
  - [mask_unused_space](../m/mask_unused_space.md) (masks unused page space)
  - BTPageGetOpaque (gets btree-specific page metadata)
  - P_ISLEAF (checks if page is a leaf page)
  - [mask_lp_flags](../m/mask_lp_flags.md) (masks line pointer flags)
  - BTP_HAS_GARBAGE (btree flag for garbage presence)
  - BTP_SPLIT_END (btree flag for split end)

- Called from (representative examples):
  - SizeOfBtreeNewroot (referenced in nbtxlog.h)

## Notes and Other Information
- This function is crucial for WAL consistency checking, ensuring that legitimate differences between primary and standby pages don't cause false consistency check failures
- The masking is necessary because certain btree operations like _bt_killitems() and _bt_check_unique() can modify flags without generating WAL records
- The BTP_HAS_GARBAGE flag masking is important because garbage collection hints are not logged and can differ between servers
- During btree page splits, the BTP_SPLIT_END flag and cycle_id handling differs between normal operation and WAL replay, necessitating their masking
- Located in src/backend/access/nbtree/nbtxlog.c:1091-1127