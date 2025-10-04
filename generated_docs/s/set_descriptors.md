# set_descriptors

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:47-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L47-L53)

## Overview
A static function that sets the thread-local descriptor list for the current thread in the ECPG library.

## Definition
```c
static void set_descriptors(struct descriptor *value)
```

## Detailed Description
The `set_descriptors` function stores a descriptor structure in thread-local storage for the current thread. It uses `pthread_setspecific` to associate the provided descriptor value with the current thread using the previously initialized `descriptor_key`. This function is the counterpart to `get_descriptors` and enables each thread to maintain its own private descriptor list. When called, it replaces any existing descriptor list for the current thread with the new value.

## Parameters / Member Variables
- `value`: A pointer to the descriptor structure to be stored as thread-local data for the current thread. Can be NULL to clear the thread-local descriptor list.

## Dependencies
- Functions called/Symbols referenced:
  - [pthread_setspecific](../p/pthread_setspecific.md)
  - [descriptor](../d/descriptor.md) (pthread key variable)
- Called from (representative examples):
  - [ECPGdeallocate_desc](../E/ECPGdeallocate_desc.md)
  - [ECPGallocate_desc](../E/ECPGallocate_desc.md)

## Notes and Other Information
- This function is declared as static, meaning it has internal linkage and is only accessible within the descriptor.c compilation unit
- The function provides the mechanism for establishing thread-local descriptor storage, which is essential for thread-safe ECPG operations
- Setting the value to NULL effectively clears the thread-local descriptor list for the current thread
- This function works in conjunction with `get_descriptors` to provide complete thread-local descriptor management
- The descriptor key must be properly initialized (via `descriptor_key_init`) before this function can be used safely

## Simplified Source

```c
static void set_descriptors(struct descriptor *value) {
    // Store descriptor list in thread-local storage
    pthread_setspecific(descriptor_key, value);
}
```