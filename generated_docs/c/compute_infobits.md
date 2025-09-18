# compute_infobits

## Location
src/backend/access/heap/heapam.c: 2686 - 2707

## Overview
compute_infobits extracts and converts specific tuple header information bits from infomask/infomask2 into a compact format for WAL record storage in heap operations.

## Definition


## Detailed Description
This static function converts selected bits from a tuple's infomask and infomask2 fields into a compressed 8-bit representation suitable for storage in WAL records. It specifically extracts lock-related and update-related bits that need to be preserved for recovery operations. The function maps heap-specific infomask bits (HEAP_*) to their corresponding WAL record bits (XLHL_*). This compression is necessary because WAL records have space constraints and only need to preserve the essential transaction state information for proper recovery and replay.

## Parameters / Member Variables
- : The primary infomask field from the tuple header containing transaction and locking information
- : The secondary infomask field containing additional tuple state information

## Dependencies
- Functions called/Symbols referenced:
  - HEAP_XMAX_IS_MULTI (infomask bit)
  - HEAP_XMAX_LOCK_ONLY (infomask bit)
  - HEAP_XMAX_EXCL_LOCK (infomask bit)
  - HEAP_XMAX_KEYSHR_LOCK (infomask bit)
  - HEAP_KEYS_UPDATED (infomask2 bit)
  - XLHL_XMAX_IS_MULTI (WAL record bit)
  - XLHL_XMAX_LOCK_ONLY (WAL record bit)
  - XLHL_XMAX_EXCL_LOCK (WAL record bit)
  - XLHL_XMAX_KEYSHR_LOCK (WAL record bit)
  - XLHL_KEYS_UPDATED (WAL record bit)
- Called from:
  - [heap_delete](../h/heap_delete.md)
  - [heap_update](../h/heap_update.md)
  - [heap_lock_tuple](../h/heap_lock_tuple.md)
  - [heap_lock_updated_tuple_rec](../h/heap_lock_updated_tuple_rec.md)
  - [heap_abort_speculative](../h/heap_abort_speculative.md)
  - [log_heap_update](../l/log_heap_update.md)

## Notes and Other Information
- This is a static function, only accessible within heapam.c
- Intentionally ignores HEAP_XMAX_SHR_LOCK bit as noted in the comment
- Works as the counterpart to fix_infomask_from_infobits() function
- Used in WAL records: xl_heap_delete, xl_heap_update, xl_heap_lock, xl_heap_lock_updated
- Returns a uint8 containing the essential transaction state bits needed for WAL replay
- The bit mapping ensures that WAL records contain sufficient information to restore proper tuple visibility and locking state during recovery