# TidStoreIsMember

## Location
src/backend/access/common/tidstore.c: 432 - 481

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
  - ItemPointerGetBlockNumber
  - ItemPointerGetOffsetNumber
  - shared_ts_find (radix tree generated function)
  - local_ts_find (radix tree generated function)
  - BlocktableEntry
  - NUM_FULL_OFFSETS
  - WORDNUM, BITNUM (macros)
  - bitmapword
- Called from (representative examples):
  - vac_tid_reaped (in vacuum.c)
  - check_set_block_offsets (in test_tidstore.c)

## Notes and Other Information
- Returns false if no entry exists for the TID's block number
- Handles both storage formats: direct offset storage in header and bitmap representation
- For header storage (nwords == 0): searches through directly stored offsets
- For bitmap storage (nwords > 0): uses bit manipulation to check the specific offset bit
- Efficiently searches both shared and local TidStore configurations
- Used primarily in vacuum operations to check if a TID has been processed
- Return type is bool: true if TID is found, false otherwise