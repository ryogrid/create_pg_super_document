# dsa_get_total_size

## Location
[src/backend/utils/mmgr/dsa.c:1027-1042](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1027-L1042)

## Overview
Returns the total size in bytes of all active segments currently allocated in a dynamic shared memory area.

## Definition
```c
size_t dsa_get_total_size(dsa_area *area)
```

## Detailed Description
This function provides a way to query the current total memory usage of a dynamic shared memory area by returning the sum of all active segments. The function acquires an exclusive lock on the area to ensure a consistent read of the total segment size value from the area's control structure.

The returned size represents only the currently active segments and does not include segments that have been freed but may still be attached by some backends. This gives an accurate picture of the memory currently in use by the DSA area.

## Parameters / Member Variables
- `area`: Pointer to the dynamic shared memory area whose total size is being queried

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)  
  - DSA_AREA_LOCK
- Called from (representative examples):
  - [RT_MEMORY_USAGE](../R/RT_MEMORY_USAGE.md) (in radixtree.h)

## Notes and Other Information
- The function uses exclusive locking to ensure atomic access to the total_segment_size field
- Returns only the size of active segments, not freed segments that may still be attached
- The size is measured in bytes and represents virtual memory usage
- This function is useful for monitoring memory usage and implementing memory management policies

## Simplified Source

```c
size_t dsa_get_total_size(dsa_area *area) {
    size_t size;

    // Acquire lock to read total size atomically
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    size = area->control->total_segment_size;
    LWLockRelease(DSA_AREA_LOCK(area));

    return size;
}
```