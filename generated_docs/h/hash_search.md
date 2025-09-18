# hash_search

## Location
src/backend/utils/hash/dynahash.c: 956 - 968

## Overview
Performs hash table operations (lookup, insertion, or removal) by computing the hash value and delegating to hash_search_with_hash_value.

## Definition


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
  - LocalBufferAlloc
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