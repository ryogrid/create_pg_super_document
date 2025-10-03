# mXactCacheGetBySet

## Location
[src/backend/access/transam/multixact.c:1611-1653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L1611-L1653)

## Overview
Searches the backend-local MultiXact cache for an existing MultiXactId that matches a given set of transaction members, returning the cached ID or InvalidMultiXactId if not found.

## Definition

```c
static MultiXactId
mXactCacheGetBySet(int nmembers, MultiXactMember *members)
```
## Detailed Description
This function performs a cache lookup to find a MultiXactId corresponding to a specific set of transaction members. It is designed to optimize MultiXact usage by allowing multiple operations with identical member sets to reuse the same MultiXactId, which is particularly beneficial for scenarios like multiple transactions locking the same large table.

The function sorts the input members array in-place using mxactMemberComparator to ensure consistent comparison with cached entries. It then iterates through the cache (MXactCache) using a doubly-linked list, comparing each cache entry's member set with the provided members. When a match is found, the cache entry is moved to the head of the list (LRU optimization) and the corresponding MultiXactId is returned.

The cache comparison assumes that cached entries are already sorted and that unused bits in the status field are zeroed, allowing for efficient memcmp-based comparison.

## Parameters / Member Variables
- `nmembers`: Number of MultiXactMember structures in the members array
- `*members`: Array of MultiXactMember structures representing the transaction set to look up (modified in-place by sorting)
## Dependencies
- Functions called/Symbols referenced:
  - qsort (with mxactMemberComparator)
  - dclist_foreach, dclist_container, dclist_move_head (doubly-linked list operations)
  - memcmp (for member array comparison)
  - debug_elog3, debug_elog2 (debugging output)
  - [mxid_to_string](mxid_to_string.md) (debugging helper)
- Called from (representative examples):
  - [MultiXactIdCreateFromMembers](../M/MultiXactIdCreateFromMembers.md) (main usage for MultiXact creation optimization)
  - debug_elog6 (debugging context)

## Notes and Other Information
- The function modifies the input members array by sorting it in-place - callers should be aware of this side effect
- Uses LRU optimization by moving found cache entries to the head of the cache list
- Particularly useful for optimizing scenarios where multiple transactions perform similar locking operations on the same large tables
- Cache entries are assumed to be pre-sorted and have zeroed unused status bits for efficient comparison
- Returns InvalidMultiXactId when no matching cache entry is found
- The cache is backend-local and transaction-scoped, being cleared at transaction end

## Simplified Source

```c
static MultiXactId
mXactCacheGetBySet(int nmembers, MultiXactMember *members)
{
    dlist_iter iter;

    debug_elog3(DEBUG2, "CacheGet: looking for %s",
                mxid_to_string(InvalidMultiXactId, nmembers, members));

    // Sort members array for consistent comparison
    qsort(members, nmembers, sizeof(MultiXactMember), mxactMemberComparator);

    // Search through cache entries
    dclist_foreach(iter, &MXactCache)
    {
        mXactCacheEnt *entry = dclist_container(mXactCacheEnt, node, iter.cur);

        // Quick check: member count must match
        if (entry->nmembers != nmembers)
            continue;

        // Compare member arrays (cache entries are pre-sorted with zeroed unused bits)
        if (memcmp(members, entry->members, nmembers * sizeof(MultiXactMember)) == 0)
        {
            debug_elog3(DEBUG2, "CacheGet: found %u", entry->multi);

            // Move to head for LRU optimization
            dclist_move_head(&MXactCache, iter.cur);
            return entry->multi;
        }
    }

    debug_elog2(DEBUG2, "CacheGet: not found :-(");
    return InvalidMultiXactId;
}
```