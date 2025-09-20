# AutoVacWorkerMain

## Location
[src/backend/postmaster/autovacuum.c:1359-1588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L1359-L1588)

## Overview
The main entry point for autovacuum worker processes that initializes the worker environment, connects to a database, and performs autovacuum operations.

## Definition

```c
struct in shared memory.  We must do this
	 * before we can use LWLocks or access any shared memory.
	 */
	InitProcess();
```
## Detailed Description
The  function serves as the complete lifecycle manager for autovacuum worker processes. It handles the entire initialization sequence, database connection, and cleanup for worker processes spawned by the postmaster at the request of the autovacuum launcher.

The function performs several critical initialization steps:

1. **Process Setup**: Establishes the process type as B_AUTOVAC_WORKER, releases postmaster memory context, and sets up signal handlers for proper worker operation including SIGINT for vacuum cancellation and SIGTERM for clean shutdown.

2. **Security Configuration**: Applies security-hardened configuration settings including an empty search_path to prevent malicious code execution, disabling zero_damaged_pages, and forcing timeouts to zero to prevent maintenance interruption.

3. **Transaction Isolation**: Forces READ COMMITTED isolation level to minimize overhead and avoid deadlocks, and disables synchronous replication for anti-wraparound maintenance.

4. **Worker Registration**: Retrieves worker information from shared memory, registers itself in the running workers list, and notifies the launcher of successful startup.

5. **Database Connection**: Connects to the assigned database using InitPostgres with override flags to ignore datallowconn restrictions, reports the connection to pgstat, and sets up the process display.

6. **Vacuum Execution**: Calls do_autovacuum() to perform the actual vacuum and analyze operations on the selected database.

The function includes comprehensive error handling with sigsetjmp/longjmp to ensure clean exit on any errors, and uses proc_exit(0) for normal termination which triggers cleanup callbacks.

## Parameters / Member Variables
- : Startup data passed from postmaster (expected to be NULL/empty for autovacuum workers)
- : Length of startup data (expected to be 0 for autovacuum workers)

## Dependencies
- Functions called/Symbols referenced:
  -  (release postmaster context)
  - / (process display management)
  -  (processing mode transitions)
  -  (signal handler registration)
  - / (process and backend initialization)
  -  (security and performance configuration)
  - / (worker information access)
  -  (worker list management)
  -  (cleanup callback registration)
  -  (statistics reporting)
  -  (database connection with overrides)
  - / (transaction ID management)
  -  (actual vacuum operations)
  -  (process termination)

- Called from (representative examples):
  - child_process_kind dispatch mechanism (src/backend/postmaster/launch_backend.c:189)

## Notes and Other Information
- Implements comprehensive security hardening by forcing safe configuration values
- Uses sigsetjmp/longjmp for robust error handling and clean exit on failures
- Handles the case where no worker slot is available (race condition) by logging and exiting gracefully
- Reports autovac startup to pgstat before database connection to update last_autovac_time even on connection failures
- Designed to prevent "stuck" autovacuum scenarios on unopenable databases
- Critical for maintaining database health through automated vacuum and analyze operations
- Integrates with PostgreSQL's process management and shared memory infrastructure
- The worker notifies the launcher of successful startup via SIGUSR2 signal