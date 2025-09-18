# hash_get_shared_size

## Location
src/backend/utils/hash/dynahash.c: 855 - 865

## Overview
Computes the required initial memory allocation for a shared-memory hashtable's control structures.

## Definition
```c
Size hash_get_shared_size(HASHCTL *info, int flags)
```

## Detailed Description
The `hash_get_shared_size` function calculates the memory requirement for the core control structures of a shared memory hash table. Unlike `hash_estimate_size` which estimates the total footprint including elements, this function focuses on the fixed-size control structures: the hash header (HASHHDR) and the directory array. It is used during the initial allocation phase to determine how much shared memory to allocate for the hash table's management structures.

The function enforces strict requirements for shared memory hash tables, ensuring that the directory size is specified and matches the maximum directory size, indicating that the directory cannot be expanded after creation.

## Parameters / Member Variables
- `info`: Pointer to HASHCTL structure containing hash table configuration parameters
- `flags`: Bit flags indicating which configuration parameters are valid and should be used

## Dependencies
- Functions called/Symbols referenced:
  - HASH_DIRSIZE (flag indicating directory size is specified)
  - [HASHHDR](../H/HASHHDR.md) (hash table header structure)
  - HASHSEGMENT (segment pointer type for directory entries)
- Called from (representative examples):
  - ShmemInitHash (during shared memory hash table setup)

## Notes and Other Information
- Requires HASH_DIRSIZE flag to be set in the flags parameter
- Enforces that info->dsize equals info->max_dsize for shared memory tables
- Only calculates space for control structures, not for actual hash elements
- Used in conjunction with other sizing functions for complete memory planning
- Critical for shared memory allocation accuracy - under-allocation would cause failures
- The calculated size covers non-expandable structures only