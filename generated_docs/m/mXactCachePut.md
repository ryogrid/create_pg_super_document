# mXactCachePut

## Location
[src/backend/access/transam/multixact.c:1701-1745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L1701-L1745)

## Overview
Adds a new MultiXactId and its composing member set to the backend-local cache, creating the cache context if necessary and managing cache size limits.

## Definition

```c
static void
mXactCachePut(MultiXactId multi, int nmembers, MultiXactMember *members)
```
## Detailed Description
This function stores a MultiXactId and its associated member set in the backend-local MultiXact cache. The cache is designed to avoid repeated SLRU area accesses for known MultiXacts during a transaction's lifetime.

The function first initializes the MXactContext memory context if it doesn't exist, creating it as a child of TopTransactionContext to ensure automatic cleanup at transaction end. It then allocates a cache entry using a flexible array member structure that includes space for the exact number of MultiXactMember structures needed.

After copying the member data, the function sorts the members array using mxactMemberComparator to ensure compatibility with mXactCacheGetBySet's comparison logic. The new entry is added to the head of the cache's doubly-linked list.

To prevent unbounded cache growth, the function implements a simple LRU eviction policy: if the cache exceeds MAX_CACHE_ENTRIES (256), it removes the least recently used entry from the tail of the list and frees its memory.

## Parameters / Member Variables
- `multi`: The MultiXactId to cache
- `nmembers`: Number of MultiXactMember structures in the members array
- `*members`: Array of MultiXactMember structures representing the transaction set to cache
## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (memory context creation)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (cache entry allocation)
  - memcpy (member data copying)
  - qsort (with mxactMemberComparator for sorting)
  - [dclist_push_head](../d/dclist_push_head.md), dclist_count, dclist_tail_node, dclist_delete_from (doubly-linked list operations)
  - [pfree](../p/pfree.md) (memory cleanup for evicted entries)
  - debug_elog3, debug_elog2 (debugging output)
  - [mxid_to_string](mxid_to_string.md) (debugging helper)
- Called from (representative examples):
  - [MultiXactIdCreateFromMembers](../M/MultiXactIdCreateFromMembers.md) (caching newly created MultiXacts)
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md) (caching retrieved MultiXact data)
  - debug_elog6 (debugging context)

## Notes and Other Information
- Creates the MXactContext memory context as a child of TopTransactionContext, ensuring automatic cleanup at transaction end
- Sorts the member array in the cache entry to maintain consistency with cache lookup operations
- Implements LRU eviction policy with a maximum of 256 cache entries (MAX_CACHE_ENTRIES)
- Cache entries are added to the head of the doubly-linked list, with eviction happening from the tail
- The cache is backend-local and transaction-scoped - it persists only for the duration of the current transaction
- Memory allocation uses the flexible array member pattern to allocate exactly the required space for each entry
- Evicted cache entries are properly freed to prevent memory leaks within the transaction context

## Simplified Source

```c
static void
mXactCachePut(MultiXactId multi, int nmembers, MultiXactMember *members)
{
    mXactCacheEnt *entry;

    debug_elog3(DEBUG2, "CachePut: storing %s",
                mxid_to_string(multi, nmembers, members));

    // Initialize cache context if needed
    if (MXactContext == NULL) {
        debug_elog2(DEBUG2, "CachePut: initializing memory context");
        MXactContext = AllocSetContextCreate(TopTransactionContext,
                                             "MultiXact cache context",
                                             ALLOCSET_SMALL_SIZES);
    }

    // Allocate cache entry with flexible array for members
    entry = MemoryContextAlloc(MXactContext,
                               offsetof(mXactCacheEnt, members) +
                               nmembers * sizeof(MultiXactMember));

    // Fill entry data
    entry->multi = multi;
    entry->nmembers = nmembers;
    memcpy(entry->members, members, nmembers * sizeof(MultiXactMember));

    // Sort members for consistency with lookup operations
    qsort(entry->members, nmembers, sizeof(MultiXactMember), mxactMemberComparator);

    // Add to head of cache list
    dclist_push_head(&MXactCache, &entry->node);

    // Evict oldest entry if cache is full
    if (dclist_count(&MXactCache) > MAX_CACHE_ENTRIES) {
        dlist_node *node = dclist_tail_node(&MXactCache);
        dclist_delete_from(&MXactCache, node);

        entry = dclist_container(mXactCacheEnt, node, node);
        debug_elog3(DEBUG2, "CachePut: pruning cached multi %u", entry->multi);
        pfree(entry);
    }
}
```