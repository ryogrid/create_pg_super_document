# TerminateOtherDBBackends

## Location
src/backend/storage/ipc/procarray.c: 3827 - 3941

## Overview
Terminates existing connections to a specified database, typically used by the DROP DATABASE command when the user has requested to forcefully drop the database.

## Definition
```c
void TerminateOtherDBBackends(Oid databaseId)
```

## Detailed Description
This function forcefully terminates all backend processes connected to the specified database, excluding the current backend process. It is primarily used by the DROP DATABASE command with the FORCE option. The function performs comprehensive permission checks before terminating any processes and fails completely if prepared transactions exist for the target database or if permission checks fail for any connection.

The function operates in several phases:
1. Scans the process array to identify backends connected to the target database
2. Collects process IDs while checking for prepared transactions
3. Validates permissions for terminating each identified process
4. Sends SIGTERM signals to terminate the processes

## Parameters / Member Variables
- `databaseId`: The OID of the database whose connections should be terminated

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease (for process array synchronization)
  - lappend_int (for building list of process IDs)
  - get_database_name (for error reporting)
  - BackendPidGetProc (for process lookup)
  - superuser/superuser_arg (for permission checks)
  - has_privs_of_role (for role privilege validation)
  - kill (for sending termination signals)
- Called from (representative examples):
  - dropdb (in dbcommands.c)

## Notes and Other Information
- The current backend is always ignored; caller must check if current backend uses the target database
- Fails immediately if any prepared transactions exist for the target database
- Permission checks are more relaxed than pg_terminate_backend, allowing termination of autovacuum workers and background workers
- Uses SIGTERM signal, with process group signaling on systems with setsid()
- Race condition exists between process identification and termination, but is considered acceptable
- Function is atomic in permission checking - if any process cannot be terminated due to permissions, the entire operation fails