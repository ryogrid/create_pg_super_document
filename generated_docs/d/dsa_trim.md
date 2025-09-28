# dsa_trim

## Location
[src/backend/utils/mmgr/dsa.c:1043-1087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1043-L1087)

## Overview
Aggressively frees all spare memory in a dynamic shared memory area in an attempt to return DSM segments back to the operating system.

## Definition
```c
void dsa_trim(dsa_area *area)
```

## Detailed Description
This function performs aggressive memory trimming by scanning all size class pools in reverse order to find and destroy entirely empty superblocks. The goal is to return as much memory as possible to the operating system by eliminating unused segments.

The function processes size classes in reverse order (from highest to lowest) so that spans-of-spans are processed last, potentially allowing them to become entirely free while processing other pools. For each size class (except large objects), it searches through fullness class 1 where entirely empty superblocks are expected to be found. When an empty superblock is detected (where nallocatable equals nmax), it is destroyed to free the underlying memory.

Large objects (DSA_SCLASS_SPAN_LARGE) are skipped because they already return segments aggressively when freed.

## Parameters / Member Variables
- `area`: Pointer to the dynamic shared memory area to be trimmed

## Dependencies
- Functions called/Symbols referenced:
  - DSA_SCLASS_LOCK
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - DsaPointerIsValid
  - [dsa_get_address](dsa_get_address.md)
  - [destroy_superblock](destroy_superblock.md)
- Called from (representative examples):
  - Available through DSA public interface

## Notes and Other Information
- Processes size classes in reverse order to maximize memory reclamation efficiency
- Only searches fullness class 1 where empty superblocks are typically located
- Large objects are excluded as they already perform aggressive freeing
- Uses exclusive locking on each size class to ensure thread-safe operations
- Empty superblocks in other fullness classes are automatically returned by dsa_free
- This is an expensive operation that should be used judiciously when memory pressure is high

## Simplified Source

```c
// Simplified version of dsa_trim
void
dsa_trim(dsa_area *area) {
    int size_class;

    // Process size classes in reverse order (highest to lowest)
    // This ensures spans-of-spans are processed last
    for (size_class = DSA_NUM_SIZE_CLASSES - 1; size_class >= 0; --size_class) {
        dsa_area_pool *pool = &area->control->pools[size_class];
        dsa_pointer span_pointer;

        // Skip large objects - they already free aggressively
        if (size_class == DSA_SCLASS_SPAN_LARGE) {
            continue;
        }

        // Look for empty superblocks in fullness class 1
        LWLockAcquire(DSA_SCLASS_LOCK(area, size_class), LW_EXCLUSIVE);
        span_pointer = pool->spans[1];

        while (DsaPointerIsValid(span_pointer)) {
            dsa_area_span *span = dsa_get_address(area, span_pointer);
            dsa_pointer next = span->nextspan;

            // If superblock is completely empty, destroy it
            if (span->nallocatable == span->nmax) {
                destroy_superblock(area, span_pointer);
            }

            span_pointer = next;
        }

        LWLockRelease(DSA_SCLASS_LOCK(area, size_class));
    }
}
```

Key simplifications made:
- Added clear comments explaining the reverse processing order strategy
- Simplified the loop structure while preserving the core algorithm
- Highlighted the key decision point (empty superblock detection)
- Emphasized the exclusive locking pattern for thread safety
- Focused on the core pattern: iterate pools → find empty superblocks → destroy them