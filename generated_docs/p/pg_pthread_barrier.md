# pg_pthread_barrier

## Location
src/include/port/pg_pthread.h: 24 - 30

## Overview
The pg_pthread_barrier struct is PostgreSQL's custom implementation of a thread barrier mechanism for platforms that lack the POSIX pthread_barrier_t type, particularly macOS.

## Definition


## Detailed Description
This structure implements a thread synchronization barrier using a sense-reversal algorithm. The barrier allows a specified number of threads to wait until all threads have reached the barrier point before any thread can proceed. The implementation uses a combination of a mutex for critical section protection and a condition variable for thread blocking and signaling.

The "sense" field implements a phase-flip mechanism where the barrier alternates between two states (true/false) with each use, allowing the barrier to be reused without reinitializing. This is more efficient than resetting counters and avoids race conditions that could occur with simple counter-based approaches.

The barrier is designed to be a drop-in replacement for pthread_barrier_t on systems where it's not available natively, maintaining API compatibility with the POSIX standard.

## Parameters / Member Variables
- : A boolean flag that alternates between true and false with each barrier cycle, implementing the sense-reversal algorithm to distinguish between barrier phases
- : The total number of threads that must arrive at the barrier before any thread is allowed to proceed
- : The current number of threads that have reached the barrier and are waiting
- : POSIX mutex used to protect the barrier's internal state from race conditions during concurrent access
- : POSIX condition variable used to block waiting threads and signal all threads when the barrier is released

## Dependencies
- Functions called/Symbols referenced:
  - [pthread_mutex_t](pthread_mutex_t.md) (POSIX mutex type)
  - pthread_cond_t (POSIX condition variable type)
  - [bool](../b/bool.md) (C99 boolean type)
- Called from (representative examples):
  - [pthread_barrier_init](pthread_barrier_init.md) (src/port/pthread_barrier_wait.c:19)
  - [pthread_barrier_wait](pthread_barrier_wait.md) (src/port/pthread_barrier_wait.c:38)
  - [pthread_barrier_destroy](pthread_barrier_destroy.md) (src/port/pthread_barrier_wait.c:72)
  - pgbench threading implementation (src/bin/pgbench/pgbench.c)

## Notes and Other Information
- This implementation is only compiled and used on platforms that lack native pthread_barrier_t support (controlled by HAVE_PTHREAD_BARRIER_WAIT configuration)
- The structure is aliased as pthread_barrier_t to provide transparent compatibility with POSIX barrier APIs
- The sense-reversal algorithm ensures that the barrier can be reused multiple times without reinitialization
- The implementation follows the POSIX pthread_barrier specification, returning PTHREAD_BARRIER_SERIAL_THREAD to exactly one thread when the barrier is released
- Used primarily in PostgreSQL's parallel processing components and benchmarking tools like pgbench
- The barrier supports the standard lifecycle: init → multiple wait cycles → destroy