# check_worker_status

## Location
[src/test/modules/test_shm_mq/setup.c:306-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_shm_mq/setup.c#L306-L323)

## Overview
A static utility function that checks the status of all background workers in a worker state structure, determining if any workers have died or if the postmaster has died.

## Definition


## Detailed Description
This function is part of PostgreSQL's test infrastructure for shared memory message queues (test_shm_mq module). It iterates through all background workers managed by the provided worker state and checks their current status using the background worker management API. The function serves as a health check mechanism to detect worker failures during testing scenarios involving multiple background processes communicating through shared memory message queues.

The function uses  to query each worker's status and immediately returns  if any worker has stopped () or if the postmaster process has died (). This provides an early failure detection mechanism for test scenarios where worker stability is critical.

## Parameters / Member Variables
- : Pointer to a worker_state structure containing:
  - : Number of background workers to check
  - : Array of BackgroundWorkerHandle pointers for each worker

## Dependencies
- Functions called/Symbols referenced:
  - : Queries the current status and PID of a background worker
  - : Enum type representing worker status states
  - : Background worker status constant indicating the worker has stopped
  - : Background worker status constant indicating postmaster death
  - : System type for process IDs

- Called from (representative examples):
  - : Uses this function to detect worker failures while waiting for workers to become ready

## Notes and Other Information
- This is a static function local to setup.c in the test_shm_mq module
- Returns  if all workers are still running,  if any worker has died or postmaster has died
- Part of PostgreSQL's testing infrastructure, not core database functionality
- Used specifically for testing shared memory message queue functionality between background workers
- The function provides fail-fast behavior - it returns immediately upon detecting the first failed worker rather than checking all workers
- Located in src/test/modules/test_shm_mq/setup.c:306-323