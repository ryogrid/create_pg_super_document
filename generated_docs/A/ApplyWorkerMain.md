# ApplyWorkerMain

## Location
[src/backend/replication/logical/worker.c:4745-4764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4745-L4764)

## Overview
Main entry point function for PostgreSQL logical replication apply workers that orchestrates the complete worker lifecycle from initialization to termination.

## Definition

```c
void
ApplyWorkerMain(Datum main_arg)
```
## Detailed Description
This function serves as the main entry point for logical replication apply worker background processes. It follows PostgreSQL's background worker pattern, accepting a Datum argument that contains the worker slot number. The function executes a straightforward sequence:

1. **Argument Processing**: Extracts the worker slot number from the Datum parameter
2. **Initialization State**: Sets a global flag indicating worker initialization is in progress
3. **Common Setup**: Calls SetupApplyOrSyncWorker to perform shared initialization tasks
4. **Initialization Complete**: Clears the initialization flag
5. **Main Processing**: Calls run_apply_worker to begin replication processing
6. **Clean Exit**: Terminates the process with success status

The function is designed to be called by PostgreSQL's background worker framework and manages the complete lifecycle of an apply worker process. The InitializingApplyWorker flag allows other parts of the system to detect when a worker is still in initialization phase.

## Parameters / Member Variables
- : Datum containing the worker slot number as an integer, passed by the background worker framework

Global variables modified:
- : Boolean flag set to true during initialization, false once setup is complete

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md): Extract integer worker slot from Datum parameter
  - [SetupApplyOrSyncWorker](../S/SetupApplyOrSyncWorker.md): Perform common worker initialization and setup
  - [run_apply_worker](../r/run_apply_worker.md): Execute the main replication processing loop
  - [proc_exit](../p/proc_exit.md): Clean process termination with exit code 0
- Called from:
  - [BackgroundWorkerHandle](../B/BackgroundWorkerHandle.md): PostgreSQL's background worker management system
  - LOGICALWORKER_H: Referenced in header file for external declarations

## Notes and Other Information
- This is a public function that serves as the official entry point for apply workers
- Follows PostgreSQL's background worker convention with Datum parameter
- The function is designed to never return under normal circumstances - it either runs indefinitely or exits the process
- InitializingApplyWorker flag is critical for preventing race conditions during worker startup
- Worker slot number passed as argument identifies which logical replication worker slot to use
- Clean exit with code 0 indicates successful worker termination (though workers typically run until shutdown)
- Function signature matches requirements for PostgreSQL background worker main functions
- Part of the larger logical replication infrastructure that enables real-time data replication between PostgreSQL instances