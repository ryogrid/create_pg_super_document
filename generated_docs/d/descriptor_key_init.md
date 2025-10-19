# descriptor_key_init

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:34-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L34-L39)

## Overview
A static initialization function that creates a pthread-specific key for thread-local descriptor storage in the ECPG library.

## Definition
```c
static void descriptor_key_init(void)
```

## Detailed Description
The `descriptor_key_init` function initializes a pthread-specific key (`descriptor_key`) that enables thread-local storage for PostgreSQL descriptor objects. This function uses `pthread_key_create` to create a unique key that can be used to associate descriptor data with individual threads. The function also registers the `descriptor_destructor` as the cleanup function that will be automatically called when a thread exits, ensuring proper cleanup of thread-local descriptor resources.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - pthread_key_create
  - [descriptor_destructor](descriptor_destructor.md)
  - [descriptor](descriptor.md) (key variable)
- Called from (representative examples):
  - [get_descriptors](../g/get_descriptors.md)

## Notes and Other Information
- This function is declared as static, meaning it has internal linkage and is only accessible within the descriptor.c compilation unit
- The function is typically called once during initialization to set up thread-local storage for descriptors
- The pthread key creation enables each thread to maintain its own set of descriptors without interference from other threads
- The destructor callback ensures automatic cleanup when threads terminate, preventing memory leaks
- This follows the POSIX threads model for thread-specific data management

## Simplified Source

```c
static void descriptor_key_init(void) {
    // Create thread-local storage key with destructor for cleanup
    pthread_key_create(&descriptor_key, descriptor_destructor);
}
```