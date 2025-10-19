# SH_DELETE

## Location
[src/include/lib/simplehash.h:857-927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/simplehash.h#L857-L927)

## Overview
A macro that defines the public hash table deletion function name using the SH_MAKE_NAME naming convention for PostgreSQL's generic simple hash table implementation.

## Definition


Function signature (after macro expansion):


## Detailed Description
SH_DELETE is a macro that expands to create a function name for the public hash table deletion operation. This is part of PostgreSQL's generic simple hash table implementation that uses C macros to generate type-specific hash table functions.

The generated function implements a sophisticated deletion algorithm:
1. Computes the hash value for the provided key
2. Performs linear probing to find the entry with matching key
3. If found, decrements the member count and performs backward shifting
4. Uses backward shifting instead of tombstones to maintain hash table efficiency
5. Shifts subsequent entries backward until an empty slot or an element at its optimal position is encountered

The backward shifting approach avoids the need for tombstone markers, keeping the hash table compact and maintaining good performance characteristics even after many deletions.

## Parameters / Member Variables
- : Pointer to the hash table structure
- : The key to delete from the hash table

Return value:
-  if the key was found and successfully deleted
-  if the key was not present in the hash table

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (for name generation)
  - SH_HASH_KEY (computes hash value for the key)
  - [SH_INITIAL_BUCKET](SH_INITIAL_BUCKET.md) (calculates starting bucket)
  - SH_COMPARE_KEYS (compares keys for equality)
  - [SH_NEXT](SH_NEXT.md) (moves to next bucket in probe sequence)
  - [SH_ENTRY_HASH](SH_ENTRY_HASH.md) (gets hash value from entry)
- Called from (representative examples):
  - PostgreSQL subsystems that need to remove entries from hash tables

## Notes and Other Information
- Uses backward shifting instead of tombstones for efficient space utilization
- The backward shifting algorithm maintains hash table density and performance
- Contains a TODO comment about potential optimization to return false early if distance is too large
- Returns a boolean indicating whether the deletion was successful
- Part of the generic simple hash table implementation that generates type-specific functions
- The deletion algorithm ensures the hash table remains compact without gaps
- More complex than typical hash table deletion due to the sophisticated shifting strategy

## Simplified Source

```c
SH_SCOPE bool
SH_DELETE(SH_TYPE *tb, SH_KEY_TYPE key)
{
    // Compute hash and find starting position
    uint32 hash = SH_HASH_KEY(tb, key);
    uint32 startelem = SH_INITIAL_BUCKET(tb, hash);
    uint32 curelem = startelem;

    // Search for the key to delete
    while (true) {
        SH_ELEMENT_TYPE *entry = &tb->data[curelem];

        // If empty bucket found, key doesn't exist
        if (entry->status == SH_STATUS_EMPTY) {
            return false;
        }

        // If key matches, delete this entry
        if (entry->status == SH_STATUS_IN_USE &&
            SH_COMPARE_KEYS(tb, hash, key, entry)) {

            SH_ELEMENT_TYPE *lastentry = entry;
            tb->members--;

            // Backward shift following elements to eliminate gaps
            while (true) {
                curelem = SH_NEXT(tb, curelem, startelem);
                SH_ELEMENT_TYPE *curentry = &tb->data[curelem];

                // If empty or at optimal position, stop shifting
                if (curentry->status != SH_STATUS_IN_USE) {
                    lastentry->status = SH_STATUS_EMPTY;
                    break;
                }

                uint32 curhash = SH_ENTRY_HASH(tb, curentry);
                uint32 curoptimal = SH_INITIAL_BUCKET(tb, curhash);

                if (curoptimal == curelem) {
                    lastentry->status = SH_STATUS_EMPTY;
                    break;
                }

                // Shift entry backward to fill gap
                memcpy(lastentry, curentry, sizeof(SH_ELEMENT_TYPE));
                lastentry = curentry;
            }

            return true;
        }

        // Continue searching
        curelem = SH_NEXT(tb, curelem, startelem);
    }
}
```

**What it does:** This function deletes an entry from the hash table by key. It searches for the key using linear probing, and when found, removes it and uses backward shifting to eliminate gaps. Instead of leaving tombstones, it shifts subsequent entries backward until it reaches an empty slot or an entry that's already at its optimal position. This maintains hash table density and performance even after deletions.