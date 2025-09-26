# win32_pthread_once

## Location
[src/interfaces/ecpg/ecpglib/misc.c:464-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/misc.c#L464-L481)

## Overview
A Windows-specific implementation of the pthread_once mechanism that ensures a given function is executed exactly once across multiple threads in ECPG library.

## Definition

```c
void
win32_pthread_once(volatile pthread_once_t *once, void (*fn) (void))
```
## Detailed Description
This function provides a pthread_once equivalent for Windows systems in the ECPG library. It implements the "call once" semantics using a mutex-based double-checked locking pattern. The function ensures that the provided function pointer  is executed exactly once, even when called concurrently from multiple threads. This is typically used for one-time initialization operations that need thread-safe execution guarantees.

The implementation uses a global mutex  to synchronize access and employs double-checked locking for performance optimization - checking the once flag before acquiring the mutex and again after acquiring it to avoid unnecessary synchronization in subsequent calls.

## Parameters / Member Variables
- : A volatile pointer to a pthread_once_t flag that tracks whether the initialization has already occurred
- : A function pointer to the initialization function that should be called exactly once

## Dependencies
- Functions called/Symbols referenced:
  - [pthread_mutex_lock](../p/pthread_mutex_lock.md)
  - [pthread_mutex_unlock](../p/pthread_mutex_unlock.md)
  - pthread_once_t (type)
- Called from (representative examples):
  - pthread_once (macro/wrapper function)
  - PTHREAD_ONCE_INIT (initialization constant)

## Notes and Other Information
This function is part of the Windows pthread compatibility layer in ECPG. It's only compiled on Windows systems where native pthread_once is not available. The double-checked locking pattern provides good performance characteristics while maintaining thread safety. The use of volatile keyword on the once parameter ensures proper memory visibility across threads.