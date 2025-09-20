# am_leader_apply_worker

## Location
[src/include/replication/worker_internal.h:340-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/worker_internal.h#L340-L346)

## Overview
A convenience function that determines if the current logical replication worker is operating as a leader apply worker (main apply worker).

## Definition

```c
static inline bool
am_leader_apply_worker(void)
```
## Detailed Description
The  function checks whether the current logical replication worker is configured as a leader apply worker. This function provides a type-safe way to determine if the current worker is the main apply worker, as opposed to a table synchronization worker or parallel apply worker.

Leader apply workers are responsible for coordinating the overall logical replication process for a subscription. They handle the main replication stream and can spawn parallel apply workers for improved performance when parallel processing is enabled.

The function includes an assertion to ensure that the worker is currently in use before checking its type. It then compares the worker type against  to determine if this is the leader apply worker.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  (assertion macro)
  -  (global variable)
  -  (enum value)

- Called from (representative examples):
  -  (src/backend/replication/logical/applyparallelworker.c:268)
  -  (src/backend/replication/logical/applyparallelworker.c:1506)
  -  (src/backend/replication/logical/applyparallelworker.c:1620)
  -  (src/backend/replication/logical/launcher.c:760)
  -  (src/backend/replication/logical/worker.c:3865)
  -  (src/backend/replication/logical/worker.c:3908)
  -  (src/backend/replication/logical/worker.c:4631)
  -  (src/backend/replication/logical/worker.c:4696)
  -  (src/backend/replication/logical/worker.c:4797)

## Notes and Other Information
- This is an inline function defined in the header file src/include/replication/worker_internal.h
- The function includes an assertion to verify the worker is in use, making it safer than direct type checking
- Leader apply workers are the primary workers in logical replication and are responsible for coordinating with parallel apply workers
- The function is commonly used in parallel apply worker logic to distinguish between leader and parallel workers
- Unlike the other worker type checking functions, this one includes an assertion for additional safety