# pthread_mutex_lock

## Location
[src/interfaces/ecpg/ecpglib/misc.c:435-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/misc.c#L435-L452)

## Overview
A Windows-specific implementation of POSIX pthread_mutex_lock that provides thread-safe mutex locking using Windows Critical Sections with lazy initialization for PostgreSQL's ECPG library.

## Definition
```c
int pthread_mutex_lock(pthread_mutex_t *mp)
```

## Detailed Description
This function implements the POSIX pthread_mutex_lock functionality for Windows platforms using Windows Critical Sections. It features lazy initialization of the critical section - if the mutex hasn't been properly initialized (initstate != 1), it performs thread-safe initialization using InterlockedExchange operations. The function uses a state machine with three states: 0 (uninitialized), 1 (initialized), and 2 (currently being initialized by another thread). When multiple threads attempt to initialize simultaneously, other threads wait via Sleep(0) until initialization is complete.

## Parameters / Member Variables
- `mp`: Pointer to a pthread_mutex_t structure representing the mutex to lock

## Dependencies
- Functions called/Symbols referenced:
  - [pthread_mutex_t](pthread_mutex_t.md) (mutex structure type)
  - InterlockedExchange (Windows atomic operation)
  - Sleep (Windows sleep function)
  - InitializeCriticalSection (Windows function)
  - EnterCriticalSection (Windows function)
- Called from (representative examples):
  - [ecpg_get_connection](../e/ecpg_get_connection.md) (in src/interfaces/ecpg/ecpglib/connect.c)
  - [ECPGconnect](../E/ECPGconnect.md) (in src/interfaces/ecpg/ecpglib/connect.c)
  - [ECPGdisconnect](../E/ECPGdisconnect.md) (in src/interfaces/ecpg/ecpglib/connect.c)
  - [ECPGdebug](../E/ECPGdebug.md) (in src/interfaces/ecpg/ecpglib/misc.c)
  - [ecpg_log](../e/ecpg_log.md) (in src/interfaces/ecpg/ecpglib/misc.c)
  - Multiple libpq functions for thread-safe operations

## Notes and Other Information
- Windows-specific implementation using Critical Sections instead of POSIX mutexes
- Implements lazy initialization - critical section is created on first use
- Uses atomic operations (InterlockedExchange) for thread-safe state management
- Sleep(0) yields the current thread's time slice when waiting for initialization
- Always returns 0 (success) following POSIX convention
- Part of PostgreSQL's pthread compatibility layer for Windows
- Widely used throughout ECPG and libpq for thread synchronization
- The initstate field tracks initialization: 0=uninitialized, 1=ready, 2=initializing