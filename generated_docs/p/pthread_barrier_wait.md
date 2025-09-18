# pthread_barrier_wait

## Location
[src/port/pthread_barrier_wait.c:38-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pthread_barrier_wait.c#L38-L71)

## Overview
Waits at a synchronization barrier until all expected threads have arrived, implementing a sense-reversing barrier algorithm.

## Definition


## Detailed Description
This function implements the core barrier synchronization logic. When a thread calls this function, it increments the arrived counter and waits at the barrier until exactly `count` threads (as specified during initialization) have called pthread_barrier_wait(). The last thread to arrive becomes the "master" thread that releases all waiting threads.

The implementation uses a sense-reversing algorithm where the `sense` field alternates between true and false on each barrier cycle. This allows the same barrier to be reused multiple times without race conditions. Non-master threads wait in a loop checking for the sense to change, while the master thread flips the sense and broadcasts to wake all waiters.

## Parameters / Member Variables
- `barrier`: Pointer to the initialized pthread_barrier_t structure

## Dependencies
- Functions called/Symbols referenced:
  - [pthread_mutex_lock](pthread_mutex_lock.md)
  - [pthread_mutex_unlock](pthread_mutex_unlock.md)
  - pthread_cond_wait
  - pthread_cond_broadcast
  - PTHREAD_BARRIER_SERIAL_THREAD
  - Assert
- Called from (representative examples):
  - THREAD_BARRIER_WAIT (macro in pgbench.c)

## Notes and Other Information
- Returns PTHREAD_BARRIER_SERIAL_THREAD (-1) for the last thread to arrive (master thread)
- Returns 0 for all other threads
- The master thread is responsible for resetting the arrived counter and flipping the sense
- Uses condition variable to avoid busy waiting
- The sense-reversing technique prevents race conditions when reusing the barrier
- All threads must use the same barrier object for proper synchronization