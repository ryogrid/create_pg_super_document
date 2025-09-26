# auto_mem_key_init

## Location
src/interfaces/ecpg/ecpglib/memory.c: 82 - 87

## Overview
Initializes a pthread thread-specific data key for automatic memory management in ECPG library.

## Definition
```c
static void
auto_mem_key_init(void)
```

## Detailed Description
This function is a one-time initialization function that creates a pthread thread-specific storage key (`auto_mem_key`) with an associated destructor function (`auto_mem_destructor`). The key is used to store a linked list of automatically allocated memory blocks that are specific to each thread. This mechanism ensures that memory allocated through ECPG's automatic allocation functions is properly tracked and cleaned up when threads terminate.

The function is called through `pthread_once()` to ensure it executes exactly once, regardless of how many threads call the related memory functions.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - pthread_key_create
  - auto_mem_destructor (as callback)
  - auto_mem_key (global variable)
- Called from (representative examples):
  - get_auto_allocs (via pthread_once)

## Notes and Other Information
- This is a static function, only accessible within the memory.c file
- Part of ECPG's thread-safe automatic memory management system
- The created key associates each thread with its own list of allocated memory blocks
- The destructor function ensures proper cleanup when threads exit
- Uses POSIX threads (pthread) API for thread-local storage