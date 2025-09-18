# InjectionPointSharedState

## Location
[src/test/modules/injection_points/injection_points.c:71-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L71-L84)

## Overview
A shared memory structure that maintains global state for injection point synchronization and wait operations across PostgreSQL processes.

## Definition


## Detailed Description
The  structure provides shared memory infrastructure for coordinating injection point waits and wakeups between different PostgreSQL processes. This structure is used by the injection point testing framework to enable synchronization scenarios where test code needs to wait for specific conditions or coordinate between multiple processes.

The structure is allocated in a dynamic shared memory segment named "injection_points" and is accessible across all PostgreSQL backend processes. It supports up to  (8) concurrent wait operations, each identified by a unique name and associated with a counter that tracks wakeup events.

The primary use case is for testing scenarios where one process needs to wait at an injection point until another process signals that it should continue, enabling complex multi-process testing scenarios.

## Parameters / Member Variables
- : A spinlock () that protects concurrent access to all other fields in the structure, ensuring thread-safe operations across multiple processes
- : An array of  counters (size ) that increment each time  is called for a corresponding wait point, used to track wakeup events
- : A two-dimensional character array storing the names of injection points associated with each wait counter slot, with each name limited to  (64) characters
- : A condition variable () used for implementing the actual wait and wakeup mechanism between processes

## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](../s/slock_t.md) (spinlock type)
  - ConditionVariable (condition variable type)
  - INJ_MAX_WAIT (constant: 8)
  - INJ_NAME_MAXLEN (constant: 64)
- Called from (representative examples):
  - [injection_point_init_state](../i/injection_point_init_state.md)
  - [injection_init_shmem](../i/injection_init_shmem.md)

## Notes and Other Information
- This structure is allocated in dynamic shared memory using  with the segment name "injection_points"
- The structure is initialized by the  callback which sets up the spinlock, zeros the counters and names, and initializes the condition variable
- The maximum number of concurrent wait points is limited to 8 ()
- Each injection point name is limited to 64 characters ()
- The shared state enables complex testing scenarios where processes can coordinate through named wait points
- This is part of PostgreSQL's testing infrastructure and is not used in production code