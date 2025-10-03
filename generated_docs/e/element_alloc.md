# element_alloc

## Location
[src/backend/utils/hash/dynahash.c:1666-1715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1666-L1715)

## Overview
Allocates new hash table elements and links them into the specified free list for dynamic hash tables.

## Definition

```c
static bool
element_alloc(HTAB *hashp, int nelem, int freelist_idx)
```
## Detailed Description
This internal function is responsible for expanding the capacity of dynamic hash tables by allocating a batch of new hash elements and linking them into the appropriate free list. The function calculates the required memory size for each element (including both the HASHELEMENT header and user data), allocates memory for all requested elements in a single allocation, and then chains them together in a linked list structure. For partitioned hash tables, it properly handles mutex locking to ensure thread-safe access to the free list.

The function operates only on non-fixed hash tables and uses the hash table's custom allocator function for memory management. Each element is properly aligned using MAXALIGN to ensure correct memory layout.

## Parameters / Member Variables
- `*hashp`: Pointer to the HTAB structure representing the hash table
- `nelem`: Number of new elements to allocate
- `freelist_idx`: Index of the free list where new elements should be linked
## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN (memory alignment macro)
  - IS_PARTITIONED (macro to check if hash table is partitioned)
  - SpinLockAcquire (for thread synchronization)
  - SpinLockRelease (for thread synchronization)
- Data structures referenced:
  - [HTAB](../H/HTAB.md) (hash table structure)
  - [HASHHDR](../H/HASHHDR.md) (hash table header)
  - [HASHELEMENT](../H/HASHELEMENT.md) (hash element structure)
- Called from (representative examples):
  - [get_hash_entry](../g/get_hash_entry.md)
  - [hash_create](../h/hash_create.md)

## Notes and Other Information
- Returns false if the hash table is fixed-size or if memory allocation fails
- Uses the hash table's custom allocator (hashp->alloc) for memory management
- Sets CurrentDynaHashCxt to ensure proper memory context
- For partitioned hash tables, acquires spinlock on the specific free list to ensure thread safety
- Elements are linked in reverse order (last allocated becomes first in free list)
- Memory layout: each element consists of a HASHELEMENT header followed by user data, both properly aligned

## Simplified Source

```c
// Simplified version of element_alloc
static bool element_alloc(HTAB *hashp, int nelem, int freelist_idx) {
    HASHHDR *hctl = hashp->hctl;
    Size elementSize;
    HASHELEMENT *firstElement;
    HASHELEMENT *currentElement;
    HASHELEMENT *prevElement;
    int i;

    // Core logic step 1: Check if hash table allows expansion
    if (hashp->isfixed) {
        return false;
    }

    // Core logic step 2: Calculate memory needed per element
    elementSize = MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(hctl->entrysize);

    // Core logic step 3: Allocate memory for all elements at once
    CurrentDynaHashCxt = hashp->hcxt;
    firstElement = (HASHELEMENT *) hashp->alloc(nelem * elementSize);

    if (!firstElement) {
        return false;
    }

    // Core logic step 4: Link all new elements into a chain (reverse order)
    prevElement = NULL;
    currentElement = firstElement;
    for (i = 0; i < nelem; i++) {
        currentElement->link = prevElement;
        prevElement = currentElement;
        currentElement = (HASHELEMENT *) (((char *) currentElement) + elementSize);
    }

    // Core logic step 5: Thread-safely add the chain to the free list
    if (IS_PARTITIONED(hctl)) {
        SpinLockAcquire(&hctl->freeList[freelist_idx].mutex);
    }

    firstElement->link = hctl->freeList[freelist_idx].freeList;
    hctl->freeList[freelist_idx].freeList = prevElement;

    if (IS_PARTITIONED(hctl)) {
        SpinLockRelease(&hctl->freeList[freelist_idx].mutex);
    }

    return true;
}
```

Key simplifications made:
- Renamed `tmpElement` to `currentElement` for clarity
- Added step-by-step comments explaining the core algorithm
- Maintained all essential functionality including thread safety
- Preserved the reverse-order linking logic which is important for performance
- Kept the fixed-size check and memory allocation failure handling