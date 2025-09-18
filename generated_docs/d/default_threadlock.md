# default_threadlock

## Location
[src/interfaces/libpq/fe-connect.c:7745-7761](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7745-L7761)

## Overview
Provides a default thread synchronization mechanism for libpq using pthread mutexes when no custom thread locking function is registered.

## Definition
```c
static void default_threadlock(int acquire)
```

## Detailed Description
This function implements the default thread locking mechanism for libpq using POSIX pthread mutexes. It serves as a fallback when applications don't provide their own thread locking functions via PQregisterThreadLock(). The function uses a single static mutex to provide thread safety for libpq operations. It follows the pgthreadlock_t API convention where a non-zero acquire parameter means lock, and zero means unlock. Since the pgthreadlock_t API doesn't provide error return conventions, mutex failures trigger assertions.

## Parameters / Member Variables
- `acquire`: Integer flag indicating the operation (non-zero = lock, zero = unlock)

## Dependencies
- Functions called/Symbols referenced:
  - [pthread_mutex_t](../p/pthread_mutex_t.md) (POSIX mutex type)
  - PTHREAD_MUTEX_INITIALIZER (POSIX mutex initializer macro)
  - [pthread_mutex_lock](../p/pthread_mutex_lock.md) (POSIX mutex lock function)
  - [pthread_mutex_unlock](../p/pthread_mutex_unlock.md) (POSIX mutex unlock function)
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - internalPQconninfoOption (as thread lock callback)
  - [PQregisterThreadLock](../P/PQregisterThreadLock.md) (as default implementation)

## Notes and Other Information
- Static function scope limited to fe-connect.c
- Uses a single static mutex (singlethread_lock) for all libpq synchronization
- Mutex is statically initialized with PTHREAD_MUTEX_INITIALIZER
- API design maintains consistency even when threading is not required
- Mutex operation failures are treated as fatal errors (Assert)
- Such failures are reportedly nonexistent in practice
- Part of libpq's thread safety infrastructure
- Replaced by custom functions when applications call PQregisterThreadLock()