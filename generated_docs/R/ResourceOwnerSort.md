# ResourceOwnerSort

## Location
src/backend/utils/resowner/resowner.c: 284 - 339

## Overview
The  function sorts all resources owned by a ResourceOwner in reverse release priority order, consolidating resources from both the fixed-size array and hash table into a single sorted array for efficient cleanup processing.

## Definition


## Detailed Description
This function implements a comprehensive sorting strategy for resource elements within a ResourceOwner, handling the dual storage approach used by the resource management system. The function operates differently depending on whether the hash table is in use:

**When only the fixed-size array is used** (nhash == 0):
- Directly sorts the fixed-size array (owner->arr) in place

**When the hash table is in use** (nhash > 0):
1. **Compaction**: Removes empty slots from the hash table by moving all valid entries to the beginning of the array, eliminating gaps
2. **Consolidation**: Moves all entries from the fixed-size array into the compacted hash table
3. **Cleanup**: Resets the fixed-size array count (narr) to 0 and updates the hash count (nhash) to reflect the total number of items
4. **Final Sort**: Sorts the consolidated hash table array

The result is always a single, contiguous, sorted array containing all resources, ordered by release priority and phase using the  comparison function. This ordering ensures proper dependency management during resource cleanup operations.

## Parameters / Member Variables
- : Pointer to the ResourceOwner structure whose resources need to be sorted

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwner (struct type)
  - ResourceElem (struct type)
  - resource_priority_cmp (comparison function for sorting)
  - qsort (standard C library sorting function)
- Called from (representative examples):
  - ResourceOwnerReleaseInternal (during resource cleanup operations)

## Notes and Other Information
- This is a static function, only accessible within the resowner.c compilation unit
- The function includes assertions to verify that the hash table has sufficient capacity to hold all elements from the fixed-size array (guaranteed by RESOWNER_HASH_MAX_ITEMS design constraint)
- After sorting, the ResourceOwner effectively switches from using both storage mechanisms to using only the hash table array
- The compaction step is necessary because hash tables typically have empty slots due to their open addressing scheme
- Critical for ensuring deterministic resource cleanup order, which is essential for maintaining data consistency during transaction rollback and error recovery
- The sorting operation prepares resources for release in the correct dependency order, preventing resource leaks and maintaining system stability