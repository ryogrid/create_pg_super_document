# AutoVacWorkerFailed

## Location
src/backend/postmaster/autovacuum.c: 1337 - 1343

## Overview
A signal function called by the postmaster to notify the autovacuum launcher when worker process creation fails.

## Definition


## Detailed Description
The  function provides a communication mechanism between the postmaster and the autovacuum launcher when a worker process cannot be created. This function is called by the postmaster when  or other process creation mechanisms fail during autovacuum worker startup.

The function works by setting a signal flag in the shared memory structure  to . This flag is monitored by the autovacuum launcher process, which can then take appropriate action such as cleaning up the worker slot that was allocated but never used.

After calling this function, the postmaster is expected to send a  signal to the autovacuum launcher to ensure it checks the signal flags promptly and handles the failure condition.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  (signal array index constant)
  -  (shared memory structure for autovacuum)

- Called from (representative examples):
  -  (src/backend/postmaster/postmaster.c:4035)

## Notes and Other Information
- This is a public function (not static) as it's called from the postmaster module
- Works in conjunction with the SIGUSR2 signal mechanism to ensure timely notification
- Critical for maintaining consistency between allocated worker slots and actual running processes
- Part of the error handling infrastructure that prevents resource leaks in the autovacuum system
- The signal flag is cleared by the launcher after it processes the failure notification
- Helps maintain the integrity of the worker pool when system resources are constrained