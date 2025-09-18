# fix_infomask_from_infobits

## Location
src/backend/access/heap/heapam.c: 9498 - 9518

## Overview
Converts compressed infobits from WAL records back into the appropriate HEAP_* flags in tuple headers during PostgreSQL recovery operations.

## Definition
```c
static void fix_infomask_from_infobits(uint8 infobits, uint16 *infomask, uint16 *infomask2)
```

## Detailed Description
This function serves as the reverse operation of `compute_infobits()` during WAL recovery. When WAL records are written, certain tuple header flags are compressed into a compact `infobits` field to save space. During recovery, this function expands those compressed bits back into the proper `infomask` and `infomask2` fields of the tuple header.

The function first clears relevant bits in both infomask fields, then selectively sets them based on the compressed infobits. This ensures a clean transition from the WAL representation back to the tuple header representation during recovery operations.

## Parameters / Member Variables
- `infobits`: Compressed 8-bit field from WAL record containing encoded tuple state information
- `infomask`: Pointer to 16-bit tuple header infomask field to be updated
- `infomask2`: Pointer to 16-bit tuple header infomask2 field to be updated

## Dependencies
- Functions called/Symbols referenced:
  - HEAP_XMAX_IS_MULTI: Multi-transaction flag for infomask
  - HEAP_XMAX_LOCK_ONLY: Lock-only transaction flag
  - HEAP_XMAX_KEYSHR_LOCK: Key-shared lock flag
  - HEAP_XMAX_EXCL_LOCK: Exclusive lock flag
  - HEAP_KEYS_UPDATED: Keys updated flag for infomask2
  - XLHL_XMAX_IS_MULTI: WAL compressed multi-transaction bit
  - XLHL_XMAX_LOCK_ONLY: WAL compressed lock-only bit
  - XLHL_XMAX_EXCL_LOCK: WAL compressed exclusive lock bit
  - XLHL_XMAX_KEYSHR_LOCK: WAL compressed key-shared lock bit
  - XLHL_KEYS_UPDATED: WAL compressed keys updated bit

- Called from (representative examples):
  - heap_xlog_delete: During delete operation recovery
  - heap_xlog_update: During update operation recovery
  - heap_xlog_lock: During tuple lock recovery
  - heap_xlog_lock_updated: During lock-updated operation recovery

## Notes and Other Information
- This is a static utility function used exclusively during WAL recovery
- Acts as the inverse of `compute_infobits()` to decompress WAL record data
- Note that HEAP_XMAX_SHR_LOCK is explicitly not handled by this function
- Essential for maintaining proper tuple header state consistency during recovery
- The function ensures clean bit transitions by clearing relevant bits before setting new ones
- Part of the space optimization strategy used in PostgreSQL WAL logging