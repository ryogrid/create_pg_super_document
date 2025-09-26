# auto_mem

## Location
[src/interfaces/ecpg/ecpglib/memory.c:65-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/memory.c#L65-L74)

## Overview
A simple linked list node structure used by the ECPG library to track memory allocations that should be automatically freed when a thread terminates.

## Definition
```c
struct auto_mem
{
    void       *pointer;
    struct auto_mem *next;
};
```

## Detailed Description
The `auto_mem` structure is a core component of PostgreSQL's ECPG (Embedded SQL in C) library's automatic memory management system. It implements a linked list where each node tracks a memory allocation made on behalf of the user through ECPG functions. This structure enables the library to automatically clean up all allocated memory when a thread exits, preventing memory leaks in multi-threaded ECPG applications.

The structure is used in conjunction with pthread thread-local storage (TLS) to maintain separate memory tracking lists for each thread. When memory is allocated via `ecpg_auto_alloc()` or explicitly tracked via `ecpg_add_mem()`, a new `auto_mem` node is created and added to the thread's memory list. The list is maintained as a simple singly-linked list with new allocations prepended to the head.

## Parameters / Member Variables
- `pointer`: A void pointer to the actual memory allocation that needs to be tracked and eventually freed
- `next`: Pointer to the next `auto_mem` node in the linked list, forming a chain of tracked memory allocations for the current thread

## Dependencies
- Functions called/Symbols referenced:
  - pthread_key_t (for thread-local storage)
  - pthread_once_t (for one-time initialization)
  - PTHREAD_ONCE_INIT (initialization constant)
- Called from (representative examples):
  - auto_mem_key_init (initializes pthread key)
  - get_auto_allocs (retrieves thread-local memory list)
  - set_auto_allocs (sets thread-local memory list)
  - ecpg_add_mem (adds new allocation to tracking list)
  - ECPGfree_auto_mem (frees all tracked memory and list nodes)
  - ecpg_clear_auto_mem (frees only the list structure, not tracked memory)

## Notes and Other Information
- This structure is part of ECPG's automatic memory management feature, which helps prevent memory leaks in embedded SQL applications
- The linked list is maintained per-thread using pthread thread-local storage, ensuring thread safety in multi-threaded applications  
- Memory tracked by this structure is automatically freed when a thread terminates through the `auto_mem_destructor` callback
- The structure itself is allocated using `ecpg_alloc()`, so both the tracked memory and the tracking structure need to be freed
- Two cleanup strategies are implemented: `ECPGfree_auto_mem()` frees both tracked memory and list nodes, while `ecpg_clear_auto_mem()` only frees the list structure
- Located in: src/interfaces/ecpg/ecpglib/memory.c:65-69