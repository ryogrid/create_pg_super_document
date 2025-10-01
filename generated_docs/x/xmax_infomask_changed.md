# xmax_infomask_changed

## Location
[src/backend/access/heap/heapam.c:2708-2730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L2708-L2730)

## Overview
xmax_infomask_changed compares two versions of a tuple's infomask to detect whether the Xmax-related transaction status has changed, used for validation after buffer lock reacquisition.

## Definition

```c
static inline bool
xmax_infomask_changed(uint16 new_infomask, uint16 old_infomask)
```
## Detailed Description
This inline static function performs a focused comparison of tuple infomask values to detect changes in Xmax-related transaction state. It's specifically designed for use after a buffer lock has been released and reacquired, ensuring that the tuple's transaction status remains consistent with previous observations. The function checks only the 'interesting' bits related to Xmax transaction state: multi-transaction status, lock-only flag, and lock type information. This selective comparison helps detect concurrent modifications that could affect the validity of previously made decisions about tuple operations.

## Parameters / Member Variables
- : The current infomask value after reacquiring the buffer lock
- : The previously observed infomask value before releasing the lock

## Dependencies
- Functions called/Symbols referenced:
  - HEAP_XMAX_IS_MULTI (bit flag for multi-transaction)
  - HEAP_XMAX_LOCK_ONLY (bit flag for lock-only operations)
  - HEAP_LOCK_MASK (bit mask for lock type information)
- Called from:
  - [heap_delete](../h/heap_delete.md) (multiple call sites)
  - [heap_update](../h/heap_update.md) (multiple call sites)  
  - [heap_lock_tuple](../h/heap_lock_tuple.md) (multiple call sites)

## Notes and Other Information
- This is a static inline function, optimized for performance and only accessible within heapam.c
- The function deliberately excludes the Xmax field itself from comparison - callers must check that separately
- Only checks 'interesting' bits relevant to Xmax transaction state, ignoring other infomask bits
- Used as part of concurrency control to validate tuple state consistency after lock reacquisition
- Returns true if any of the monitored Xmax-related bits have changed, false if they remain the same
- Critical for ensuring transaction isolation and preventing race conditions in heap operations

## Simplified Source

```c
static inline bool xmax_infomask_changed(uint16 new_infomask, uint16 old_infomask) {
    // Only check bits relevant to Xmax transaction state
    const uint16 interesting =
        HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_LOCK_MASK;

    // Return true if any interesting bits changed
    return (new_infomask & interesting) != (old_infomask & interesting);
}
```