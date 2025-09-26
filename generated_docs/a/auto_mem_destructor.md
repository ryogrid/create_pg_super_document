# auto_mem_destructor

## Location
src/interfaces/ecpg/ecpglib/memory.c: 75 - 81

## Overview
A static callback function that automatically cleans up thread-local memory allocations when a thread terminates in ECPG's automatic memory management system.

## Definition

```c
static void
auto_mem_destructor(void *arg)
```
## Detailed Description
The  function serves as a pthread destructor callback that is automatically invoked when a thread terminates or when the associated thread-specific data key is destroyed. This function is a critical component of ECPG's automatic memory management system, ensuring that all automatically allocated memory is properly freed when threads end, preventing memory leaks in multi-threaded applications.

The function ignores its argument parameter (which would typically contain the thread-specific data value) and instead calls  to clean up all automatic memory allocations associated with the current thread. This design ensures complete cleanup regardless of the specific data stored in the thread-local key.

This automatic cleanup mechanism is essential for embedded SQL applications that use multiple threads, as it prevents memory leaks that could occur if threads terminate without explicitly freeing their automatic memory allocations.

## Parameters / Member Variables
- : Unused parameter (thread-specific data value), cast to void to prevent compiler warnings

## Dependencies
- Functions called/Symbols referenced:
  - ECPGfree_auto_mem (frees all automatic memory allocations for the current thread)
- Called from (representative examples):
  - auto_mem_key_init (registered as pthread key destructor)

## Notes and Other Information
- This is a static function, only accessible within the memory.c file
- Serves as a pthread destructor callback function registered with pthread_key_create()
- The (void) arg cast prevents compiler warnings about unused parameters
- Automatically invoked by the pthread library when threads terminate
- Essential for preventing memory leaks in multi-threaded ECPG applications
- Part of ECPG's thread-safe automatic memory management system
- Works in conjunction with pthread thread-specific data to provide per-thread memory cleanup
- The function design follows pthread destructor callback conventions