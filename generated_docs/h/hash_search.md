# hash_search

## Location
[src/backend/utils/hash/dynahash.c:956-968](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L956-L968)

## Overview
Performs hash table operations (lookup, insertion, or removal) by computing the hash value and delegating to hash_search_with_hash_value.

## Definition

```c
void *
hash_search(HTAB *hashp,
			const void *keyPtr,
			HASHACTION action,
			bool *foundPtr)
```
## Detailed Description
This is the primary interface for hash table operations in PostgreSQL. It provides a convenient wrapper that computes the hash value internally and then calls hash_search_with_hash_value to perform the actual operation. The function supports four types of operations: finding existing entries, inserting new entries (with or without error on memory exhaustion), and removing entries. The return value and foundPtr flag provide information about whether the operation succeeded and whether an existing entry was found.

## Parameters / Member Variables
- : Pointer to the HTAB structure representing the hash table
- : Pointer to the key data for the operation
- : The type of operation to perform (HASH_FIND, HASH_ENTER, HASH_ENTER_NULL, or HASH_REMOVE)
- : Optional pointer to a boolean that will be set to indicate whether an existing entry was found

## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md) (hash table structure)
  - HASHACTION (enumeration for operation types)
  - [hash_search_with_hash_value](hash_search_with_hash_value.md) (core implementation function)
- Called from (representative examples):
  - [getmissingattr](../g/getmissingattr.md)
  - [gistMemorizeParent](../g/gistMemorizeParent.md)
  - [gistGetParent](../g/gistGetParent.md)
  - [_hash_splitbucket](_hash_splitbucket.md)
  - [ReorderBufferTXNByXid](../R/ReorderBufferTXNByXid.md)
  - [LocalBufferAlloc](../L/LocalBufferAlloc.md)
  - [LockAcquireExtended](../L/LockAcquireExtended.md)
  - [smgropen](../s/smgropen.md)
  - [WaitEventCustomNew](../W/WaitEventCustomNew.md)
  - [lookup_collation_cache](../l/lookup_collation_cache.md)

## Notes and Other Information
- Returns a pointer to the found/created/removed entry, or NULL if not found
- For HASH_REMOVE operations, the returned pointer is dangling and should not be dereferenced
- HASH_ENTER normally reports an 'out of memory' error on allocation failure
- HASH_ENTER_NULL returns NULL instead of erroring on memory exhaustion
- The foundPtr parameter is particularly useful for HASH_ENTER operations to distinguish between new and existing entries
- This function is heavily used throughout PostgreSQL for various caching and lookup operations
- Automatically computes the hash value using the table's hash function before delegating to the core implementation

## Simplified Source

```c
// Simplified version of hash_search
void *hash_search(HTAB *hashp, const void *keyPtr, HASHACTION action, bool *foundPtr) {
    // Compute hash value and delegate to core implementation
    uint32 hashvalue = hashp->hash(keyPtr, hashp->keysize);
    return hash_search_with_hash_value(hashp, keyPtr, hashvalue, action, foundPtr);
}

// Simplified version of hash_search_with_hash_value (core implementation)
void *hash_search_with_hash_value(HTAB *hashp, const void *keyPtr, uint32 hashvalue,
                                  HASHACTION action, bool *foundPtr) {
    HASHBUCKET currBucket;
    HASHBUCKET *prevBucketPtr;

    // Step 1: Check if table expansion is needed for insertions
    if (action == HASH_ENTER || action == HASH_ENTER_NULL) {
        if (should_expand_table(hashp)) {
            expand_table(hashp);
        }
    }

    // Step 2: Find the initial bucket location
    hash_initial_lookup(hashp, hashvalue, &prevBucketPtr);
    currBucket = *prevBucketPtr;

    // Step 3: Search the collision chain for matching key
    while (currBucket != NULL) {
        if (currBucket->hashvalue == hashvalue &&
            keys_match(currBucket, keyPtr, hashp->keysize)) {
            break;  // Found matching entry
        }
        prevBucketPtr = &(currBucket->link);
        currBucket = *prevBucketPtr;
    }

    // Step 4: Set found flag if requested
    if (foundPtr) {
        *foundPtr = (currBucket != NULL);
    }

    // Step 5: Perform the requested action
    switch (action) {
        case HASH_FIND:
            // Return found entry or NULL
            return currBucket ? get_entry_data(currBucket) : NULL;

        case HASH_REMOVE:
            if (currBucket) {
                // Remove from chain and add to free list
                remove_from_chain(prevBucketPtr, currBucket);
                add_to_freelist(hashp, currBucket);
                return get_entry_data(currBucket);  // Dangling pointer warning!
            }
            return NULL;

        case HASH_ENTER:
        case HASH_ENTER_NULL:
            if (currBucket) {
                // Return existing entry
                return get_entry_data(currBucket);
            }

            // Create new entry
            currBucket = allocate_new_entry(hashp);
            if (!currBucket) {
                // Handle out of memory
                if (action == HASH_ENTER_NULL) return NULL;
                report_out_of_memory_error(hashp);
            }

            // Initialize and link new entry
            setup_new_entry(currBucket, hashvalue, keyPtr, hashp);
            link_to_chain(prevBucketPtr, currBucket);

            return get_entry_data(currBucket);
    }

    // Should never reach here
    elog(ERROR, "unrecognized hash action code: %d", action);
    return NULL;
}
```

Key simplifications made:
- Abstracted low-level memory management and locking details
- Simplified the collision chain traversal logic
- Consolidated similar error handling patterns
- Removed statistics tracking code for clarity
- Focused on the main execution paths for each action type
- Used descriptive helper function names to represent complex operations
- Maintained the essential algorithm flow and all four operation types