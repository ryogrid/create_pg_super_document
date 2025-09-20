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