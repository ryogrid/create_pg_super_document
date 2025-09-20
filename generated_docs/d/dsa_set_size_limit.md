# dsa_set_size_limit

## Location
[src/backend/utils/mmgr/dsa.c:1018-1026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1018-L1026)

## Overview
Sets the total size limit for a dynamic shared memory area, controlling the maximum amount of memory that can be allocated from the operating system for new segments.

## Definition

```c
void
dsa_set_size_limit(dsa_area *area, size_t limit)
```
## Detailed Description
This function establishes a size limit for the total virtual memory usage of a dynamic shared memory area. The limit is enforced when new segments need to be allocated from the operating system. If the area has already exceeded the new limit when this function is called, there is no immediate effect - the limit will only be enforced for future allocations.

The function uses exclusive locking to ensure thread-safe modification of the area's control structure. It's important to note that the actual virtual memory usage may temporarily exceed this limit when segments have been freed but not yet detached by all backends that have attached to them.

## Parameters / Member Variables
- : Pointer to the dynamic shared memory area whose size limit is being set
- : The maximum total segment size in bytes that the area is allowed to consume

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease
  - DSA_AREA_LOCK
- Called from (representative examples):
  - StatsShmemInit (in pgstat_shmem.c)

## Notes and Other Information
- The function acquires an exclusive lock on the DSA area to ensure atomic updates to the size limit
- This is a soft limit that only affects future segment allocations, not existing allocations
- Virtual memory usage may temporarily exceed the limit due to delayed segment detachment by backends
- The limit is stored in the area's control structure as max_total_segment_size