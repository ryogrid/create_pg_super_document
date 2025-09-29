# get_hash_entry

## Location
[src/backend/utils/hash/dynahash.c:1259-1343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1259-L1343)

## Overview
Allocates a new hashtable entry from the freelist, with support for both partitioned and non-partitioned hash tables, including cross-freelist borrowing in partitioned tables when memory is constrained.

## Definition

```c
static HASHBUCKET
get_hash_entry(HTAB *hashp, int freelist_idx)
```
## Detailed Description
This internal function manages the allocation of new hash table entries by retrieving them from freelists. It implements a sophisticated allocation strategy that handles both partitioned and non-partitioned hash tables differently. For partitioned tables, it uses spinlocks to coordinate access to freelists and implements a borrowing mechanism where entries can be taken from other freelists when the requested freelist is empty.

The function operates with the following strategy:
1. First attempts to get an entry from the specified freelist
2. If that fails, tries to allocate a new chunk of buckets using element_alloc()
3. For partitioned tables, if allocation fails, it systematically borrows from other freelists
4. Returns NULL only when all options are exhausted

The borrowing mechanism is crucial for maintaining the guarantee that elements can always be allocated if the table was initially sized appropriately or if elements have been previously deleted.

## Parameters / Member Variables
- : Pointer to the hash table structure (HTAB) from which to allocate an entry
- : Index of the freelist to allocate from (relevant for partitioned tables)

## Dependencies
- Functions called/Symbols referenced:
  - IS_PARTITIONED (macro to check if hash table is partitioned)
  - SpinLockAcquire (acquires spinlock for thread safety)
  - SpinLockRelease (releases spinlock)
  - [element_alloc](../e/element_alloc.md) (allocates new chunk of bucket elements)
  - NUM_FREELISTS (constant defining number of freelists)
- Called from (representative examples):
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md) (when creating new entries)

## Notes and Other Information
- This is a static (internal) function, not exposed in the public API
- Returns NULL if out of memory, unless the underlying allocator throws errors
- For partitioned tables, proper spinlock management ensures thread safety
- The borrowing algorithm cycles through all freelists to ensure no available memory is missed
- Maintains accurate nentries counts even when borrowing between freelists
- Critical for maintaining hash table allocation guarantees in concurrent environments

## Simplified Source

```c
// Simplified version of get_hash_entry
static HASHBUCKET
get_hash_entry(HTAB *hashp, int freelist_idx)
{
    HASHHDR *hctl = hashp->hctl;
    HASHBUCKET newElement;

    for (;;)
    {
        // Step 1: Try to get an entry from the target freelist
        if (IS_PARTITIONED(hctl))
            SpinLockAcquire(&hctl->freeList[freelist_idx].mutex);

        newElement = hctl->freeList[freelist_idx].freeList;

        if (newElement != NULL)
            break;  // Found an entry, exit loop

        if (IS_PARTITIONED(hctl))
            SpinLockRelease(&hctl->freeList[freelist_idx].mutex);

        // Step 2: Try to allocate new bucket chunk
        if (!element_alloc(hashp, hctl->nelem_alloc, freelist_idx))
        {
            // Step 3: For partitioned tables, try borrowing from other freelists
            if (!IS_PARTITIONED(hctl))
                return NULL;  // Out of memory

            // Search all other freelists for available entries
            int borrow_from_idx = freelist_idx;
            for (;;)
            {
                borrow_from_idx = (borrow_from_idx + 1) % NUM_FREELISTS;
                if (borrow_from_idx == freelist_idx)
                    break;  // Checked all freelists, fail

                SpinLockAcquire(&(hctl->freeList[borrow_from_idx].mutex));
                newElement = hctl->freeList[borrow_from_idx].freeList;

                if (newElement != NULL)
                {
                    // Remove from source freelist
                    hctl->freeList[borrow_from_idx].freeList = newElement->link;
                    SpinLockRelease(&(hctl->freeList[borrow_from_idx].mutex));

                    // Count in target freelist
                    SpinLockAcquire(&hctl->freeList[freelist_idx].mutex);
                    hctl->freeList[freelist_idx].nentries++;
                    SpinLockRelease(&hctl->freeList[freelist_idx].mutex);

                    return newElement;
                }

                SpinLockRelease(&(hctl->freeList[borrow_from_idx].mutex));
            }

            return NULL;  // No entries available anywhere
        }
    }

    // Step 4: Remove entry from freelist and update counters
    hctl->freeList[freelist_idx].freeList = newElement->link;
    hctl->freeList[freelist_idx].nentries++;

    if (IS_PARTITIONED(hctl))
        SpinLockRelease(&hctl->freeList[freelist_idx].mutex);

    return newElement;
}
```

Key simplifications made:
- Added clear step-by-step comments for the main algorithm phases
- Consolidated complex logic flow into distinct sections
- Removed detailed comments about implementation rationale to focus on core logic
- Preserved essential thread safety and allocation logic
- Maintained the borrowing mechanism which is critical for partitioned tables
- Kept all return paths and error conditions intact