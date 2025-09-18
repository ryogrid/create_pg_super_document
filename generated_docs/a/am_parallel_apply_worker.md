# am_parallel_apply_worker

## Location
src/include/replication/worker_internal.h: 347 - 353

## Overview
A convenience function that determines if the current logical replication worker is operating as a parallel apply worker.

## Definition


## Detailed Description
The  function checks whether the current logical replication worker is configured as a parallel apply worker. This function provides a safe interface for determining if the current worker is one of the parallel workers that assist the leader apply worker in processing replication changes.

Parallel apply workers are spawned by the leader apply worker to improve performance by processing changes in parallel. They handle specific transactions or changes assigned by the leader worker, allowing for concurrent processing of the replication stream when parallel apply is enabled for a subscription.

The function includes an assertion to ensure that the worker is currently in use before delegating to the  macro, which checks both the worker's usage status and that its type is .

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  (assertion macro)
  -  (macro)
  -  (global variable)
  -  (enum value)

- Called from (representative examples):
  -  (src/backend/replication/logical/applyparallelworker.c:558)
  -  (src/backend/replication/logical/applyparallelworker.c:1522)
  -  (src/backend/replication/logical/applyparallelworker.c:1593)
  -  (src/backend/replication/logical/worker.c:2014)
  -  (src/backend/replication/logical/worker.c:3457)
  -  (src/backend/replication/logical/worker.c:3846)
  -  (src/backend/replication/logical/worker.c:3947)
  -  (src/backend/replication/logical/worker.c:4823)
  -  (src/backend/replication/logical/worker.c:4888)
  -  (src/backend/replication/logical/worker.c:5130)

## Notes and Other Information
- This is an inline function defined in the header file src/include/replication/worker_internal.h
- The function includes an assertion for safety, ensuring the worker is in use before type checking
- Parallel apply workers are part of PostgreSQL's parallel logical replication feature, allowing for improved performance
- These workers coordinate with the leader apply worker to process changes concurrently
- The function is heavily used in parallel apply worker logic to distinguish between different worker types
- Parallel apply workers handle specific transactions assigned by the leader worker, enabling concurrent processing of the replication stream