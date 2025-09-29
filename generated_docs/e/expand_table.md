# expand_table

## Location
[src/backend/utils/hash/dynahash.c:1511-1607](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1511-L1607)

## Overview
Expands a hash table by adding one more hash bucket, redistributing existing entries to maintain proper hash distribution.

## Definition

```c
static bool
expand_table(HTAB *hashp)
```
## Detailed Description
The expand_table function is a critical component of PostgreSQL's dynamic hash table implementation that handles table growth. When called, it adds exactly one new hash bucket to the table and redistributes existing entries between the old bucket and the newly created bucket based on their hash values. The function carefully manages the hash table's internal structures including segments, buckets, and hash masks. It allocates new segments when necessary and updates the table's masking parameters to accommodate the larger bucket space. The redistribution process ensures that only entries from one specific old bucket need to be examined and potentially moved to the new bucket, maintaining the hash table's performance characteristics.

## Parameters / Member Variables
- : Pointer to the HTAB (hash table) structure to be expanded

## Dependencies
- Functions called/Symbols referenced:
  - [dir_realloc](../d/dir_realloc.md)
  - [seg_alloc](../s/seg_alloc.md)
  - [calc_bucket](../c/calc_bucket.md)
  - IS_PARTITIONED
  - MOD
  - [HASHHDR](../H/HASHHDR.md), HASHSEGMENT, HASHBUCKET (structure access)
- Called from (representative examples):
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)

## Notes and Other Information
- Returns true on success, false on failure (typically due to memory allocation failure)
- This is a static function, only used internally within dynahash.c
- Cannot be used on partitioned hash tables (checked with Assert)
- Increments hash_expansions counter when HASH_STATISTICS is enabled
- Only one old bucket needs to be split due to the hash masking algorithm used
- Updates low_mask and high_mask when crossing power-of-2 boundaries
- Terminates rebuilt hash chains with NULL pointers to prevent corruption
- Part of the PostgreSQL dynamic hash table expansion mechanism

## Simplified Source

```c
// Simplified version of expand_table
static bool expand_table(HTAB *hashp) {
    HASHHDR *hctl = hashp->hctl;
    long new_bucket, old_bucket;
    long new_segnum, new_segndx;
    long old_segnum, old_segndx;
    HASHSEGMENT old_seg, new_seg;
    HASHBUCKET *oldlink, *newlink;
    HASHBUCKET currElement, nextElement;

    // Calculate new bucket position
    new_bucket = hctl->max_bucket + 1;
    new_segnum = new_bucket >> hashp->sshift;
    new_segndx = MOD(new_bucket, hashp->ssize);

    // Allocate new segment if needed
    if (new_segnum >= hctl->nsegs) {
        if (new_segnum >= hctl->dsize) {
            if (!dir_realloc(hashp))
                return false;  // Directory expansion failed
        }
        if (!(hashp->dir[new_segnum] = seg_alloc(hashp)))
            return false;  // Segment allocation failed
        hctl->nsegs++;
    }

    // Update bucket count
    hctl->max_bucket++;

    // Find corresponding old bucket for redistribution
    old_bucket = (new_bucket & hctl->low_mask);

    // Update hash masks if crossing power-of-2 boundary
    if ((uint32) new_bucket > hctl->high_mask) {
        hctl->low_mask = hctl->high_mask;
        hctl->high_mask = (uint32) new_bucket | hctl->low_mask;
    }

    // Get segment pointers for old and new buckets
    old_segnum = old_bucket >> hashp->sshift;
    old_segndx = MOD(old_bucket, hashp->ssize);
    old_seg = hashp->dir[old_segnum];
    new_seg = hashp->dir[new_segnum];

    // Redistribute entries between old and new buckets
    oldlink = &old_seg[old_segndx];
    newlink = &new_seg[new_segndx];

    for (currElement = *oldlink; currElement != NULL; currElement = nextElement) {
        nextElement = currElement->link;

        // Check if element belongs in old bucket or new bucket
        if ((long) calc_bucket(hctl, currElement->hashvalue) == old_bucket) {
            *oldlink = currElement;
            oldlink = &currElement->link;
        } else {
            *newlink = currElement;
            newlink = &currElement->link;
        }
    }

    // Terminate both hash chains
    *oldlink = NULL;
    *newlink = NULL;

    return true;
}
```

Key simplifications made:
- Removed HASH_STATISTICS tracking code for clarity
- Removed assertion checks and debug code
- Simplified variable declarations by grouping related types
- Added clear comments explaining each major step
- Consolidated complex pointer manipulation into clearer sections
- Focused on the core algorithm: calculate positions, allocate space, update masks, redistribute entries
- Removed detailed low-level comments in favor of high-level step descriptions