# ecpg_add_mem

## Location
[src/interfaces/ecpg/ecpglib/memory.c:117-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/memory.c#L117-L130)

## Overview
Adds a memory pointer to the thread-specific automatic memory tracking list for later cleanup.

## Definition
```c
bool
ecpg_add_mem(void *ptr, int lineno)
```

## Detailed Description
This function adds a given memory pointer to the thread-specific linked list of automatically tracked memory allocations. It creates a new `struct auto_mem` node to wrap the pointer and prepends it to the existing list head.

The function allocates memory for the tracking structure itself using `ecpg_alloc()`, then initializes the new node with the provided pointer and links it to the current list head. The list is maintained in LIFO (Last In, First Out) order, with new allocations added at the head.

This mechanism allows ECPG to automatically free tracked memory when threads terminate or when cleanup functions are explicitly called, helping to prevent memory leaks in embedded SQL applications.

## Parameters / Member Variables
- `ptr`: Pointer to the memory block that should be tracked for automatic cleanup
- `lineno`: Line number in the source code for error reporting purposes

**Return Value:**
- Returns `true` if the memory pointer was successfully added to the tracking list
- Returns `false` if allocation of the tracking structure failed

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_alloc](ecpg_alloc.md) (for tracking structure allocation)
  - [get_auto_allocs](../g/get_auto_allocs.md) (to get current list head)
  - [set_auto_allocs](../s/set_auto_allocs.md) (to update list head)
- Called from (representative examples):
  - [ecpg_auto_alloc](ecpg_auto_alloc.md) (line 108)
  - User code that needs to add existing pointers to automatic cleanup

## Notes and Other Information
- This is a public function, part of the ECPG library interface
- The function prepends new entries to the list, maintaining LIFO order
- If allocation of the tracking structure fails, the original pointer is not affected
- The tracking structure itself is also allocated using ecpg_alloc, but is not added to the auto-cleanup list
- Thread-safe: each thread maintains its own separate tracking list
- Part of ECPG's automatic memory management system for embedded SQL applications
- Can be used to add existing memory pointers to automatic cleanup, not just newly allocated ones