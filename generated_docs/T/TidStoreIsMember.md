# TidStoreIsMember

## Location
[src/backend/access/common/tidstore.c:432-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L432-L481)

## Overview
Tests whether a given TID (Tuple Identifier) is present in the TidStore, returning true if found.

## Definition
```c
bool TidStoreIsMember(TidStore *ts, ItemPointer tid)
```

## Detailed Description
TidStoreIsMember checks if a specified TID (Tuple Identifier) exists in the TidStore. It first extracts the block number and offset number from the ItemPointer, then searches for the corresponding BlocktableEntry in either the shared or local radix tree. If no entry exists for the block, it returns false. When an entry is found, it checks for the offset using one of two methods: if nwords is 0, it searches the directly stored offsets in the header; otherwise, it uses bitmap lookup by calculating the appropriate word and bit position within the bitmap representation.

## Parameters / Member Variables
- `ts`: Pointer to the TidStore object to search
- `tid`: ItemPointer (TID) to search for in the TidStore

## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIsShared (macro)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - shared_ts_find (radix tree generated function)
  - local_ts_find (radix tree generated function)
  - [BlocktableEntry](../B/BlocktableEntry.md)
  - NUM_FULL_OFFSETS
  - WORDNUM, BITNUM (macros)
  - bitmapword
- Called from (representative examples):
  - [vac_tid_reaped](../v/vac_tid_reaped.md) (in vacuum.c)
  - [check_set_block_offsets](../c/check_set_block_offsets.md) (in test_tidstore.c)

## Notes and Other Information
- Returns false if no entry exists for the TID's block number
- Handles both storage formats: direct offset storage in header and bitmap representation
- For header storage (nwords == 0): searches through directly stored offsets
- For bitmap storage (nwords > 0): uses bit manipulation to check the specific offset bit
- Efficiently searches both shared and local TidStore configurations
- Used primarily in vacuum operations to check if a TID has been processed
- Return type is bool: true if TID is found, false otherwise

## Simplified Source

```c
bool TidStoreIsMember(TidStore *ts, ItemPointer tid)
{
    BlockNumber blk = ItemPointerGetBlockNumber(tid);
    OffsetNumber off = ItemPointerGetOffsetNumber(tid);

    // Find the page entry for this block
    BlocktableEntry *page;
    if (TidStoreIsShared(ts))
        page = shared_ts_find(ts->tree.shared, blk);
    else
        page = local_ts_find(ts->tree.local, blk);

    // No entry for this block
    if (page == NULL)
        return false;

    if (page->header.nwords == 0) {
        // Offsets stored directly in header
        for (int i = 0; i < NUM_FULL_OFFSETS; i++) {
            if (page->header.full_offsets[i] == off)
                return true;
        }
        return false;
    } else {
        // Offsets stored in bitmap
        int wordnum = WORDNUM(off);
        int bitnum = BITNUM(off);

        // Check if word exists
        if (wordnum >= page->header.nwords)
            return false;

        // Check if bit is set
        return (page->words[wordnum] & ((bitmapword) 1 << bitnum)) != 0;
    }
}
```