# ShutdownPostgres

## Location
[src/backend/utils/init/postinit.c:1361-1377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L1361-L1377)

## Overview
ShutdownPostgres is a backend shutdown callback function that performs critical cleanup operations during backend process termination, ensuring proper transaction abort and user lock release.

## Definition


## Detailed Description
ShutdownPostgres serves as a backend shutdown callback that executes during the backend termination process. It is registered via before_shmem_exit() in InitPostgres to ensure it runs before lower-level modules begin their shutdown procedures.

The function performs two critical cleanup operations: it forcibly aborts any active transactions using AbortOutOfAnyTransaction(), and it releases all user-acquired locks via LockReleaseAll() with the USER_LOCKMETHOD parameter. User locks are explicitly released because they are not automatically freed by transaction end, unlike regular table locks.

This callback is designed to execute reliably even if user-level cleanup operations fail, which is why it runs as a separate callback rather than being combined with user-level cleanup routines.

## Parameters / Member Variables
- : Exit code passed to the shutdown callback (unused in this function)
- : Additional data passed to the callback (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [AbortOutOfAnyTransaction](../A/AbortOutOfAnyTransaction.md)
  - [LockReleaseAll](../L/LockReleaseAll.md)
  - USER_LOCKMETHOD
- Called from (representative examples):
  - Registered as callback in InitPostgres via before_shmem_exit()

## Notes and Other Information
- This is a static function and shutdown callback, not called directly by user code
- Registered early in InitPostgres to ensure it executes before low-level module shutdowns
- Critical for preventing resource leaks during backend termination
- Handles two distinct cleanup categories: transaction state and lock management
- User locks require explicit release because they persist beyond transaction boundaries
- Part of PostgreSQL's layered shutdown architecture that ensures proper cleanup ordering
- Executes regardless of whether user-level cleanup succeeds or fails
- Essential for maintaining system integrity during both normal and abnormal backend termination