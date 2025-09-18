# SH_FREE

## Location
src/include/lib/simplehash.h: 424 - 441

## Overview
Deallocates memory previously allocated by SH_ALLOCATE in PostgreSQL's simplehash implementation, providing the corresponding cleanup function for hash table memory management.

## Definition


## Detailed Description
This function serves as the memory deallocation counterpart to SH_ALLOCATE, providing proper cleanup of memory allocated for hash table data structures. The default implementation uses PostgreSQL's standard pfree() function, which works with memory allocated through the MemoryContext system.

Like SH_ALLOCATE, this function can be customized when SH_USE_NONDEFAULT_ALLOCATOR is defined, allowing users to provide their own memory management scheme that matches their custom allocator. The function signature remains consistent regardless of the underlying implementation, providing a clean abstraction for memory management operations.

The function is critical for preventing memory leaks during hash table destruction and resizing operations, where old data arrays need to be properly deallocated before new ones are allocated.

## Parameters / Member Variables
- : Pointer to the hash table structure (used for context in custom allocators)
- : Pointer to the memory block to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (macro for name generation)
  - [pfree](../p/pfree.md) (PostgreSQL's standard memory deallocation function)
- Called from (representative examples):
  - [SH_DESTROY](SH_DESTROY.md) (during hash table destruction to free the data array)
  - [SH_GROW](SH_GROW.md) (during resizing to free the old data array after copying to new array)

## Notes and Other Information
- This is part of the simplehash template system and expands to a function with user-defined prefix
- Must be paired with SH_ALLOCATE - memory allocated by SH_ALLOCATE should only be freed by SH_FREE
- The default implementation uses pfree(), which is safe for memory allocated through MemoryContext
- Users can define SH_USE_NONDEFAULT_ALLOCATOR to provide custom deallocation logic
- Critical for proper memory management during hash table lifecycle operations
- The type parameter allows custom allocators to maintain allocation context or statistics