# SH_ALLOCATE

## Location
src/include/lib/simplehash.h: 412 - 423

## Overview
Allocates memory for hash table data structures in PostgreSQL's simplehash implementation, providing a configurable memory allocation interface.

## Definition


## Detailed Description
This function provides memory allocation services for the simplehash template system with support for different allocation strategies. The implementation varies based on compile-time configuration:

- **With SH_RAW_ALLOCATOR defined**: Uses a user-provided raw allocator function that must zero the returned memory space
- **Without SH_RAW_ALLOCATOR (default)**: Uses PostgreSQL's MemoryContext system with MemoryContextAllocExtended(), allocating zeroed memory with support for huge allocations

The function is designed to handle the specific memory requirements of hash tables, including the need for zeroed memory (critical for proper initialization of hash table entries) and support for large allocations when hash tables grow significantly.

The SH_USE_NONDEFAULT_ALLOCATOR macro can be defined to skip the default implementation entirely, allowing users to provide their own custom allocator functions.

## Parameters / Member Variables
- : Pointer to the hash table structure containing allocation context information
- : Number of bytes to allocate

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME (macro for name generation)
  - SH_RAW_ALLOCATOR (conditionally, when defined by user)
  - MemoryContextAllocExtended (default PostgreSQL memory allocation)
  - MCXT_ALLOC_HUGE | MCXT_ALLOC_ZERO (allocation flags for large, zeroed memory)
- Called from (representative examples):
  - SH_CREATE (during initial hash table creation)
  - SH_GROW (during hash table resizing operations)

## Notes and Other Information
- This is part of the simplehash template system and expands to a function with user-defined prefix
- The allocator must return zeroed memory for proper hash table functionality
- Supports both custom allocators and PostgreSQL's standard memory context system
- Critical for hash table creation and dynamic resizing operations
- The MCXT_ALLOC_HUGE flag allows allocation of memory larger than 1GB when needed
- Users can define SH_USE_NONDEFAULT_ALLOCATOR to provide completely custom allocation logic