# get_auto_allocs

## Location
[src/interfaces/ecpg/ecpglib/memory.c:88-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/memory.c#L88-L94)

## Overview
Retrieves the thread-specific linked list of automatically allocated memory blocks for the current thread.

## Definition
```c
static struct auto_mem *
get_auto_allocs(void)
```

## Detailed Description
This function returns a pointer to the head of a linked list containing all automatically allocated memory blocks for the current thread. It uses pthread thread-specific data to maintain separate memory lists for each thread in a multi-threaded environment.

The function first ensures that the thread-specific key is initialized by calling `pthread_once()` with `auto_mem_key_init`. This guarantees that the key creation happens exactly once, even in a multi-threaded environment. After initialization, it retrieves the thread-specific data using `pthread_getspecific()` and casts it to the appropriate `struct auto_mem *` type.

## Parameters / Member Variables
This function takes no parameters.

**Return Value:**
- Returns a pointer to the head of the `struct auto_mem` linked list for the current thread
- Returns NULL if no automatic allocations have been made for this thread

## Dependencies
- Functions called/Symbols referenced:
  - pthread_once
  - [pthread_getspecific](../p/pthread_getspecific.md)
  - [auto_mem_key_init](../a/auto_mem_key_init.md) (callback function)
  - auto_mem_once (static variable)
  - auto_mem_key (static variable)
- Called from (representative examples):
  - [ecpg_add_mem](../e/ecpg_add_mem.md) (line 125)
  - [ECPGfree_auto_mem](../E/ECPGfree_auto_mem.md) (line 133)
  - [ecpg_clear_auto_mem](../e/ecpg_clear_auto_mem.md) (line 153)

## Notes and Other Information
- This is a static function, only accessible within the memory.c file
- Part of ECPG's thread-safe automatic memory management system
- Uses pthread_once pattern to ensure proper initialization in multi-threaded environments
- Each thread maintains its own separate list of allocated memory blocks
- The returned list is managed internally and should not be modified directly by callers