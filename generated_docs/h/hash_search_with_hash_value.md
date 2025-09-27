# hash_search_with_hash_value

## Location
[src/backend/utils/hash/dynahash.c:969-1145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L969-L1145)

## Overview
Core implementation of hash table operations that performs lookup, insertion, or removal using a pre-computed hash value.

## Definition

```c
void *hash_search_with_hash_value(HTAB *hashp, const void *keyPtr, uint32 hashvalue, HASHACTION action, bool *foundPtr);
```
## Detailed Description
This function provides the core implementation for all hash table operations in PostgreSQL's dynamic hash table system. It accepts a pre-computed hash value (typically from get_hash_value) and performs the requested operation efficiently. The function handles table expansion during insertion, manages collision chains through linear probing, and provides thread-safe operations for partitioned tables. It supports four primary operations: finding entries, inserting new entries (with two error-handling variants), and removing entries. The implementation includes optimizations for partitioned tables and maintains comprehensive statistics when compiled with HASH_STATISTICS.

## Parameters / Member Variables
- `hashp`: Pointer to the HTAB structure representing the hash table
- `keyPtr`: Pointer to the key data for the operation
- `hashvalue`: Pre-computed hash value for the key (should be computed using get_hash_value)
- `action`: The operation type (HASH_FIND, HASH_ENTER, HASH_ENTER_NULL, or HASH_REMOVE)
- `foundPtr`: Optional pointer to a boolean that indicates whether an existing entry was found

## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md), HASHHDR, HASHBUCKET (hash table structures)
  - HASHACTION (operation enumeration)
  - FREELIST_IDX (macro for freelist indexing)
  - IS_PARTITIONED (partitioning check macro)
  - [has_seq_scans](has_seq_scans.md) (function to check for active sequential scans)
  - [expand_table](../e/expand_table.md) (table expansion function)
  - [hash_initial_lookup](hash_initial_lookup.md) (initial bucket lookup)
  - [get_hash_entry](../g/get_hash_entry.md) (entry allocation function)
  - ELEMENTKEY (macro to access element key)
- Called from (representative examples):
  - [hash_search](hash_search.md) (convenience wrapper)
  - [BufTableLookup](../B/BufTableLookup.md), BufTableInsert, BufTableDelete
  - [LockAcquireExtended](../L/LockAcquireExtended.md), SetupLockInTable
  - Various predicate locking functions

## Notes and Other Information
- Returns a pointer to the element's key portion for found/created entries, or NULL for not found
- For HASH_REMOVE operations, the returned pointer becomes dangling after the call
- HASH_ENTER reports out-of-memory errors; HASH_ENTER_NULL returns NULL instead
- Automatically expands the table when load factor becomes too high (during insertion only)
- Table expansion is disabled for partitioned tables, frozen tables, or tables with active sequential scans
- Uses spinlocks for thread safety in partitioned tables
- Maintains collision statistics when HASH_STATISTICS is defined
- The caller is responsible for filling the data portion of newly created entries
- Critical to avoid throwing errors after successful entry creation to prevent table corruption
- Supports both shared memory and local memory allocation depending on table configuration

## Simplified Source

```c
// Simplified version of hash_search_with_hash_value
void *hash_search_with_hash_value(HTAB *hashp, const void *keyPtr, uint32 hashvalue,
                                  HASHACTION action, bool *foundPtr) {
    HASHHDR *hctl = hashp->hctl;
    int freelist_idx = FREELIST_IDX(hctl, hashvalue);
    HASHBUCKET currBucket;
    HASHBUCKET *prevBucketPtr;

    // Step 1: Check if table expansion is needed for insertions
    if (action == HASH_ENTER || action == HASH_ENTER_NULL) {
        if (should_expand_table(hctl, hashp)) {
            expand_table(hashp);
        }
    }

    // Step 2: Find the initial bucket position
    hash_initial_lookup(hashp, hashvalue, &prevBucketPtr);
    currBucket = *prevBucketPtr;

    // Step 3: Search collision chain for matching key
    while (currBucket != NULL) {
        if (currBucket->hashvalue == hashvalue &&
            keys_match(currBucket, keyPtr, hashp)) {
            break;  // Found matching entry
        }
        prevBucketPtr = &(currBucket->link);
        currBucket = *prevBucketPtr;
    }

    // Step 4: Set found status if requested
    if (foundPtr) {
        *foundPtr = (currBucket != NULL);
    }

    // Step 5: Perform the requested action
    switch (action) {
        case HASH_FIND:
            // Return found entry or NULL
            return currBucket ? ELEMENTKEY(currBucket) : NULL;

        case HASH_REMOVE:
            if (currBucket) {
                // Remove from chain and add to freelist
                remove_entry_from_chain(hctl, freelist_idx, prevBucketPtr, currBucket);
                return ELEMENTKEY(currBucket);
            }
            return NULL;

        case HASH_ENTER:
        case HASH_ENTER_NULL:
            if (currBucket) {
                // Return existing entry
                return ELEMENTKEY(currBucket);
            }

            // Create new entry
            currBucket = allocate_new_entry(hashp, freelist_idx, action);
            if (!currBucket) {
                return NULL;  // Out of memory
            }

            // Link into chain and initialize
            link_new_entry(prevBucketPtr, currBucket, hashvalue, keyPtr, hashp);
            return ELEMENTKEY(currBucket);
    }

    elog(ERROR, "unrecognized hash action code: %d", action);
    return NULL;
}

// Helper function abstractions used in simplified version:
// - should_expand_table(): Checks expansion conditions
// - keys_match(): Compares keys for equality
// - remove_entry_from_chain(): Handles entry removal with locking
// - allocate_new_entry(): Gets new entry with error handling
// - link_new_entry(): Links entry and copies key data
```

**Key simplifications made:**
- Abstracted complex expansion conditions into `should_expand_table()`
- Consolidated key comparison logic into `keys_match()`
- Extracted entry removal complexity into `remove_entry_from_chain()`
- Simplified memory allocation and error handling with `allocate_new_entry()`
- Combined entry linking and key copying into `link_new_entry()`
- Removed detailed statistics tracking and platform-specific code
- Focused on the main algorithm flow rather than low-level implementation details
- Maintained the essential four-step process: expansion check, lookup, search, action