# get_descriptors

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:40-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L40-L46)

## Overview
A static function that retrieves the thread-local descriptor list for the current thread in the ECPG library.

## Definition
```c
static struct descriptor *get_descriptors(void)
```

## Detailed Description
The `get_descriptors` function provides access to the thread-local descriptor storage for the current thread. It uses pthread-specific data mechanisms to maintain separate descriptor lists for each thread. The function employs `pthread_once` to ensure that the descriptor key is initialized exactly once across all threads, then retrieves the thread-specific descriptor data using `pthread_getspecific`. This design allows multiple threads to work with descriptors independently without data corruption or interference.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - pthread_once
  - [descriptor_key_init](../d/descriptor_key_init.md)
  - [pthread_getspecific](../p/pthread_getspecific.md)
  - [descriptor](../d/descriptor.md) (pthread key variable)
- Called from (representative examples):
  - [ECPGdeallocate_desc](../E/ECPGdeallocate_desc.md)
  - [ECPGallocate_desc](../E/ECPGallocate_desc.md)
  - [ecpg_find_desc](../e/ecpg_find_desc.md)

## Notes and Other Information
- This function is declared as static, meaning it has internal linkage and is only accessible within the descriptor.c compilation unit
- Returns a pointer to the thread-local descriptor structure, or NULL if none has been set for the current thread
- The `pthread_once` ensures thread-safe initialization of the descriptor key even in multi-threaded environments
- This function is fundamental to the thread-safe operation of ECPG descriptors, allowing each thread to maintain its own descriptor namespace
- The returned pointer should be treated as thread-specific and not shared between threads

## Simplified Source

```c
static struct descriptor *get_descriptors(void) {
    // Ensure descriptor key is initialized exactly once
    pthread_once(&descriptor_once, descriptor_key_init);

    // Return thread-specific descriptor list
    return (struct descriptor *) pthread_getspecific(descriptor_key);
}
```