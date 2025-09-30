# set_auto_allocs

## Location
[src/interfaces/ecpg/ecpglib/memory.c:95-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/memory.c#L95-L100)

## Overview
Sets the thread-specific linked list head for automatically allocated memory blocks for the current thread.

## Definition
```c
static void
set_auto_allocs(struct auto_mem *am)
```

## Detailed Description
This function updates the thread-specific storage to point to a new head of the automatically allocated memory linked list for the current thread. It uses the pthread thread-specific data mechanism to maintain separate memory tracking lists for each thread in a multi-threaded environment.

The function is typically called when adding new memory blocks to the list (setting a new head) or when clearing the entire list (setting to NULL). It works in conjunction with `get_auto_allocs()` to provide thread-safe memory management.

## Parameters / Member Variables
- `am`: Pointer to the new head of the `struct auto_mem` linked list, or NULL to clear the list

## Dependencies
- Functions called/Symbols referenced:
  - [pthread_setspecific](../p/pthread_setspecific.md)
  - auto_mem_key (static variable)
- Called from (representative examples):
  - [ecpg_add_mem](../e/ecpg_add_mem.md) (line 126)
  - [ECPGfree_auto_mem](../E/ECPGfree_auto_mem.md) (line 146) 
  - [ecpg_clear_auto_mem](../e/ecpg_clear_auto_mem.md) (line 165)

## Notes and Other Information
- This is a static function, only accessible within the memory.c file
- Part of ECPG's thread-safe automatic memory management system
- Does not perform any validation on the input parameter
- Assumes that `auto_mem_key` has been properly initialized via `auto_mem_key_init()`
- Each thread maintains its own separate storage, so this only affects the current thread
- Commonly used to update the list head when adding new allocations or to clear the list entirely

## Simplified Source

```c
static void set_auto_allocs(struct auto_mem *am) {
    // Set thread-specific automatic memory list head
    pthread_setspecific(auto_mem_key, am);
}
```