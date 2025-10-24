# pthread_barrier_init

## Location
[src/port/pthread_barrier_wait.c:19-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pthread_barrier_wait.c#L19-L37)

## Overview
Initializes a pthread barrier object that can be used to synchronize a specific number of threads at a synchronization point.

## Definition

```c
int
pthread_barrier_init(pthread_barrier_t *barrier, const void *attr, int count)
```
## Detailed Description
This function initializes a pthread barrier that implements a thread synchronization primitive. The barrier allows exactly `count` threads to wait at a synchronization point before all are released to continue execution. This is PostgreSQL's implementation of pthread barriers for systems that don't natively support them (notably macOS).

The implementation uses a sense-reversing barrier algorithm with a mutex and condition variable. The `sense` field alternates between true and false on each barrier cycle to handle reuse of the same barrier object. The `arrived` counter tracks how many threads have reached the barrier, and when it equals `count`, all waiting threads are released.

## Parameters / Member Variables
- `barrier`: Pointer to the pthread_barrier_t structure to initialize
- `attr`: Barrier attributes (currently unused, should be NULL)
- `count`: Number of threads that must call pthread_barrier_wait() before any of them will be released

## Dependencies
- Functions called/Symbols referenced:
  - pthread_cond_init
  - [pthread_mutex_init](pthread_mutex_init.md)
  - pthread_cond_destroy
- Called from (representative examples):
  - THREAD_BARRIER_INIT (macro in pgbench.c)

## Notes and Other Information
- Returns 0 on success, or an error code on failure
- This is only compiled when HAVE_PTHREAD_BARRIER_WAIT is not defined
- Part of PostgreSQL's portability layer for missing POSIX thread components
- If mutex initialization fails, the condition variable is properly cleaned up
- The barrier can be reused multiple times after initialization

## Simplified Source

```c
int pthread_barrier_init(pthread_barrier_t *barrier, const void *attr, int count) {
    // Initialize barrier state
    barrier->sense = false;     // Sense-reversing flag for reuse
    barrier->count = count;     // Total threads required
    barrier->arrived = 0;       // Threads arrived so far

    // Initialize condition variable for thread signaling
    int error = pthread_cond_init(&barrier->cond, NULL);
    if (error != 0)
        return error;

    // Initialize mutex for protecting barrier state
    error = pthread_mutex_init(&barrier->mutex, NULL);
    if (error != 0) {
        // Cleanup on failure
        pthread_cond_destroy(&barrier->cond);
        return error;
    }

    return 0;  // Success
}
```