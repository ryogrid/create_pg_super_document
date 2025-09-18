# SH_DESTROY

## Location
src/include/lib/simplehash.h: 472 - 479

## Overview
Completely destroys a PostgreSQL simplehash table, deallocating all associated memory and cleaning up resources.

## Definition


## Detailed Description
This function provides complete cleanup and destruction of a simplehash table that was previously created with SH_CREATE. It performs a two-step deallocation process:

1. **Data Array Cleanup**: Uses SH_FREE to deallocate the hash table's data array (tb->data), which contains all the hash buckets and their entries
2. **Structure Cleanup**: Uses pfree() to deallocate the hash table structure itself

The function ensures proper memory management by matching the allocation pattern used in SH_CREATE. The data array is allocated through SH_ALLOCATE and thus must be freed through SH_FREE, while the hash table structure itself is allocated through standard PostgreSQL memory allocation and freed with pfree().

This function should be called when the hash table is no longer needed to prevent memory leaks. After calling SH_DESTROY, the hash table pointer becomes invalid and should not be used.

## Parameters / Member Variables
- : Pointer to the hash table structure to be destroyed

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (macro for name generation)
  - [SH_FREE](SH_FREE.md) (to deallocate the data array)
  - [pfree](../p/pfree.md) (PostgreSQL's standard memory deallocation for the table structure)
- Called from:
  - User code when hash table cleanup is needed
  - Generally not called directly by other simplehash internal functions

## Notes and Other Information
- This is part of the simplehash template system and expands to a function with user-defined prefix
- Must be called to prevent memory leaks when a hash table is no longer needed
- The function makes the hash table pointer invalid - it should not be used after destruction
- Complements SH_CREATE in the hash table lifecycle
- The scope (SH_SCOPE) is configurable and can be static, extern, or static inline
- Part of the public API for the generated hash table implementation