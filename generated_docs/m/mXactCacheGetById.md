# mXactCacheGetById

## Location
src/backend/access/transam/multixact.c: 1654 - 1700

## Overview
Retrieves the composing MultiXactMember set from the backend-local cache for a given MultiXactId, returning a palloc'd copy of the member array if found.

## Definition


## Detailed Description
This function performs a reverse cache lookup, searching for a specific MultiXactId in the backend-local MultiXact cache and retrieving its associated member set. When a match is found, the function allocates memory using palloc() and creates a copy of the member array, which is returned to the caller via the members output parameter.

The function iterates through the cache using a doubly-linked list, comparing each entry's MultiXactId with the requested ID. When found, it calculates the required memory size, allocates space, and performs a memcpy to create a complete copy of the member array. The cache entry is then moved to the head of the list for LRU optimization.

The caller is responsible for freeing the allocated memory returned through the members parameter. This design ensures that the caller gets an independent copy that won't be affected by subsequent cache modifications.

## Parameters / Member Variables
- : The MultiXactId to look up in the cache
- : Output parameter - pointer to a pointer that will be set to the address of a palloc'd copy of the MultiXactMember array

## Dependencies
- Functions called/Symbols referenced:
  - dclist_foreach, dclist_container, dclist_move_head (doubly-linked list operations)
  - palloc (memory allocation)
  - memcpy (memory copying)
  - debug_elog3, debug_elog2 (debugging output)
  - mxid_to_string (debugging helper)
- Called from (representative examples):
  - GetMultiXactIdMembers (main usage for retrieving MultiXact composition)
  - debug_elog6 (debugging context)

## Notes and Other Information
- Returns the number of members in the MultiXactMember array on success, or -1 if the MultiXactId is not found in cache
- The caller must pfree() the allocated memory returned through the members parameter
- Uses LRU optimization by moving found cache entries to the head of the cache list
- The function modifies the doubly-linked list structure while using a non-modifiable iterator, which is acceptable because iteration exits immediately after the modification
- Provides a complete independent copy of the member array, protecting against cache modifications affecting the caller's data
- Part of the backend-local cache system that helps avoid repeated SLRU area accesses for known MultiXacts