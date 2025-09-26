# table_scan_bitmap_next_tuple

## Location
[src/include/access/tableam.h:1986-2012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1986-L2012)

## Overview
Fetches the next tuple from a bitmap table scan into a slot and returns whether a visible tuple was found.

## Definition
```c
static inline bool
table_scan_bitmap_next_tuple(TableScanDesc scan,
                            struct TBMIterateResult *tbmres,
                            TupleTableSlot *slot)
```

## Detailed Description
This function is the core tuple-fetching mechanism for bitmap heap scans in PostgreSQL. It retrieves the next visible tuple from a previously selected block and stores it in the provided tuple slot. The function works in conjunction with `table_scan_bitmap_next_block()` which must have been called first to select a valid block.

The function operates under strict preconditions: `table_scan_bitmap_next_block()` must have previously returned `true` for the current block, and no previous `table_scan_bitmap_next_tuple()` call for the same block should have returned `false`. This ensures proper sequencing in the bitmap scan process.

Like its companion function, it includes safety checks to prevent calls during logical decoding operations and delegates the actual work to the table access method's specific implementation.

## Parameters / Member Variables
- `scan`: TableScanDesc - The active bitmap scan descriptor
- `tbmres`: struct TBMIterateResult - The bitmap iteration result containing block and tuple information
- `slot`: TupleTableSlot - The destination slot where the fetched tuple will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `TransactionIdIsValid`: Validates transaction IDs
  - `CheckXidAlive`: Global variable for logical decoding transaction tracking
  - `bsysscan`: System catalog scan indicator
  - `elog`: Error logging mechanism
  - `scan->rs_rd->rd_tableam->scan_bitmap_next_tuple`: Table access method implementation

- Called from (representative examples):
  - `BitmapHeapNext`: Main bitmap heap scan executor function

## Notes and Other Information
- Must be called after `table_scan_bitmap_next_block()` has successfully selected a block
- Returns `true` if a visible tuple was found and stored in the slot, `false` otherwise
- Part of the bitmap scan optimization which allows efficient processing of multiple qualifying tuples
- Contains logical decoding safety checks similar to other bitmap scan functions
- The function is inline and defined in the table access method interface header
- Proper sequencing is critical - calling this function without proper block selection will lead to undefined behavior