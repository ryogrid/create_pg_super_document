# lockTableForWorker

## Location
src/bin/pg_dump/parallel.c: 1301 - 1335

## Overview
Acquires an ACCESS SHARE lock on a table that will be dumped by a worker process, with deadlock prevention using NOWAIT semantics.

## Definition


## Detailed Description
This function is a critical component of pg_dump's parallel processing system that prevents deadlocks when multiple worker processes attempt to access tables concurrently. The function addresses a specific deadlock scenario:

1. The leader process already holds ACCESS SHARE locks on all tables
2. An external process requests an ACCESS EXCLUSIVE lock (blocked by leader's lock)
3. A worker process requests an ACCESS SHARE lock (queued behind the exclusive lock request)
4. This creates a deadlock since the leader waits for the worker, but the server cannot detect this

To prevent infinite waits, the function uses "LOCK TABLE ... IN ACCESS SHARE MODE NOWAIT" which either succeeds immediately or fails if another process has requested an exclusive lock. If the lock cannot be acquired, the function terminates the backup with a fatal error rather than deadlocking.

The function skips locking for BLOBS entries since they don't correspond to actual database tables.

## Parameters / Member Variables
- : Archive handle containing the database connection and dump context
- : Table of Contents entry representing the table to be locked, containing namespace and tag information

## Dependencies
- Functions called/Symbols referenced:
  - fmtQualifiedId (formats qualified table identifier)
  - PQexec (executes SQL command)
  - PGRES_COMMAND_OK (success status constant)
  - TocEntry (table of contents entry structure)

- Called from (representative examples):
  - WaitForCommands (parallel worker command processing)

## Notes and Other Information
- Uses NOWAIT semantics to prevent deadlocks in parallel dump operations
- Critical for maintaining data consistency during concurrent access
- Part of pg_dump's parallel processing infrastructure
- Terminates the entire backup process if lock cannot be acquired
- Skips processing for BLOB entries which don't require table locks