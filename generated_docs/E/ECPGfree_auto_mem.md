# ECPGfree_auto_mem

## Location
src/interfaces/ecpg/ecpglib/memory.c: 131 - 150

## Overview
Frees all automatically allocated memory tracked by the ECPG (Embedded SQL in C) library for the current thread.

## Definition
```c
void ECPGfree_auto_mem(void)
```

## Detailed Description
This function is part of ECPG's automatic memory management system. It walks through a thread-local linked list of memory allocations that were made on behalf of the user and frees all of them. The function uses pthread-specific storage to maintain separate memory tracking for each thread. After freeing all tracked memory blocks, it resets the thread's auto-allocation list to NULL.

The function operates by:
1. Retrieving the current thread's auto-allocation list via `get_auto_allocs()`
2. Iterating through each `auto_mem` node in the linked list
3. Freeing both the user data pointer and the `auto_mem` structure itself
4. Resetting the thread's auto-allocation list to NULL

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - get_auto_allocs
  - auto_mem (struct)
  - ecpg_free
  - set_auto_allocs
- Called from (representative examples):
  - ecpg_raise
  - ecpg_raise_backend
  - auto_mem_destructor
  - ECPGset_var
  - SQLSTATE (macro)

## Notes and Other Information
- This function is typically called during error handling or cleanup scenarios in ECPG applications
- The function is thread-safe as it operates on thread-local storage via pthread-specific keys
- Memory tracking is automatic and transparent to the user - this function handles cleanup of memory allocated by ECPG on the user's behalf
- Used in conjunction with ECPG's automatic memory management to prevent memory leaks in embedded SQL applications
- The function gracefully handles the case where no auto-allocated memory exists (am == NULL)