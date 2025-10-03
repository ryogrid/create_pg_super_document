# hash_update_hash_key

## Location
[src/backend/utils/hash/dynahash.c:1146-1258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1146-L1258)

## Overview
Changes the hash key of an existing entry in a PostgreSQL dynamic hash table without removing the entry from memory, providing safe hash key updates even in concurrent environments.

## Definition

```c
bool
hash_update_hash_key(HTAB *hashp,
					 void *existingEntry,
					 const void *newKeyPtr)
```
## Detailed Description
This function provides a specialized operation to update the hash key of an existing hash table entry. It is equivalent to removing the entry, creating a new entry, and copying the data, but with a crucial difference: the entry never goes to the table's freelist. This design ensures that the operation cannot suffer from out-of-memory failures, even when other processes are operating in different partitions of the hashtable.

The function performs several key operations:
1. Validates that the existing entry is actually in the hash table
2. Calculates the new hash value for the new key
3. Checks if the new key would create a collision (returns false if so)  
4. Updates the hash chain links if the entry moves to a different bucket
5. Copies the new key into the existing entry and updates its hash value

The function includes special handling for frozen hashtables (updates are disallowed) and provides comprehensive collision chain management.

## Parameters / Member Variables
- `*hashp`: Pointer to the hash table structure (HTAB) being operated on
- `*existingEntry`: Pointer to the existing entry whose key should be updated
- `*newKeyPtr`: Pointer to the new key data that will replace the existing key
## Dependencies
- Functions called/Symbols referenced:
  - ELEMENT_FROM_KEY (macro for converting entry pointer to element)
  - [hash_initial_lookup](hash_initial_lookup.md) (locates the appropriate hash bucket)
  - ELEMENTKEY (macro for extracting key from hash element)
  - hashp->hash (hash function for calculating new hash value)
  - hashp->match (comparison function for key matching)
  - hashp->keycopy (function for copying key data)
- Called from (representative examples):
  - [PostPrepare_Locks](../P/PostPrepare_Locks.md) (in lock manager for updating lock entries)

## Notes and Other Information
- Returns true if successful, false if the new hash key already exists
- Throws an error if the existingEntry pointer is not actually in the table
- For partitioned hashtables, the caller must hold locks on both relevant partitions if the new key belongs to a different partition
- Currently reports false even if old and new hash keys are identical (this is intentional for existing uses)
- Includes hash statistics tracking when HASH_STATISTICS is enabled
- Disallows updates on frozen hashtables to maintain data consistency

## Simplified Source

```c
bool
hash_update_hash_key(HTAB *hashp, void *existingEntry, const void *newKeyPtr)
{
    HASHELEMENT *existingElement = ELEMENT_FROM_KEY(existingEntry);
    uint32 newhashvalue, bucket, newbucket;
    HASHBUCKET currBucket, *prevBucketPtr, *oldPrevPtr;

    // Check if hashtable is frozen (disallow updates)
    if (hashp->frozen)
        elog(ERROR, "cannot update in frozen hashtable");

    // Find existing element in its current hash chain
    bucket = hash_initial_lookup(hashp, existingElement->hashvalue, &prevBucketPtr);
    currBucket = *prevBucketPtr;

    while (currBucket != NULL) {
        if (currBucket == existingElement)
            break;
        prevBucketPtr = &(currBucket->link);
        currBucket = *prevBucketPtr;
    }

    // Verify element exists in table
    if (currBucket == NULL)
        elog(ERROR, "hash_update_hash_key argument is not in hashtable");

    oldPrevPtr = prevBucketPtr;

    // Calculate new hash value and find target bucket
    newhashvalue = hashp->hash(newKeyPtr, hashp->keysize);
    newbucket = hash_initial_lookup(hashp, newhashvalue, &prevBucketPtr);
    currBucket = *prevBucketPtr;

    // Check for collision with existing key
    while (currBucket != NULL) {
        if (currBucket->hashvalue == newhashvalue &&
            hashp->match(ELEMENTKEY(currBucket), newKeyPtr, hashp->keysize) == 0)
            break;
        prevBucketPtr = &(currBucket->link);
        currBucket = *prevBucketPtr;
    }

    // Return false if new key already exists
    if (currBucket != NULL)
        return false;

    currBucket = existingElement;

    // Update hash chain links if moving to different bucket
    if (bucket != newbucket) {
        *oldPrevPtr = currBucket->link;
        *prevBucketPtr = currBucket;
        currBucket->link = NULL;
    }

    // Update element with new key and hash value
    currBucket->hashvalue = newhashvalue;
    hashp->keycopy(ELEMENTKEY(currBucket), newKeyPtr, hashp->keysize);

    return true;
}
```