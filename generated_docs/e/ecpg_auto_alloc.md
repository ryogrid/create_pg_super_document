# ecpg_auto_alloc

## Location
src/interfaces/ecpg/ecpglib/memory.c: 101 - 116

## Overview
Allocates memory that is automatically tracked and will be freed when the thread terminates or when explicitly cleared.

## Definition
```c
char *
ecpg_auto_alloc(long size, int lineno)
```

## Detailed Description
This function allocates memory of the specified size and automatically adds it to the thread-specific list of tracked memory allocations. Unlike regular `ecpg_alloc()`, memory allocated with this function will be automatically freed when the thread terminates (via the pthread destructor mechanism) or when `ECPGfree_auto_mem()` is explicitly called.

The function first attempts to allocate memory using `ecpg_alloc()`, then adds the allocated pointer to the automatic memory tracking list using `ecpg_add_mem()`. If either operation fails, it cleans up appropriately and returns NULL.

This is particularly useful for ECPG applications where memory needs to be automatically managed across function calls and thread boundaries, reducing the risk of memory leaks in embedded SQL applications.

## Parameters / Member Variables
- `size`: The number of bytes to allocate
- `lineno`: Line number in the source code for error reporting purposes

**Return Value:**
- Returns a pointer to the allocated memory block on success
- Returns NULL if allocation fails or if the memory could not be added to the tracking list

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_alloc
  - ecpg_add_mem
  - ecpg_free
- Called from (representative examples):
  - Various ECPG-generated code and user applications requiring automatic memory management

## Notes and Other Information
- This is a public function, part of the ECPG library interface
- Memory allocated by this function is automatically freed when the thread terminates
- If `ecpg_add_mem()` fails, the function properly cleans up by freeing the allocated memory to prevent leaks
- The allocated memory is zero-initialized (via calloc in ecpg_alloc)
- Thread-safe: each thread maintains its own separate list of automatically allocated memory
- Part of ECPG's automatic memory management system for embedded SQL applications