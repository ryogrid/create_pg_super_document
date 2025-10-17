# lockTableForWorker

## Location
[src/bin/pg_dump/parallel.c:1301-1335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1301-L1335)

## Overview
Acquires an ACCESS SHARE lock on a table that will be dumped by a worker process, with deadlock prevention using NOWAIT semantics.

## Definition

```c
static void
lockTableForWorker(ArchiveHandle *AH, TocEntry *te)
```
## Detailed Description
This function is a critical component of pg_dump's parallel processing system that prevents deadlocks when multiple worker processes attempt to access tables concurrently. The function addresses a specific deadlock scenario:

1. The leader process already holds ACCESS SHARE locks on all tables
2. An external process requests an ACCESS EXCLUSIVE lock (blocked by leader's lock)
3. A worker process requests an ACCESS SHARE lock (queued behind the exclusive lock request)
4. This creates a deadlock since the leader waits for the worker, but the server cannot detect this

To prevent infinite waits, the function uses "LOCK TABLE ... IN ACCESS SHARE MODE NOWAIT" which either succeeds immediately or fails if another process has requested an exclusive lock. If the lock cannot be acquired, the function terminates the backup with a fatal error rather than deadlocking.

The function skips locking for BLOBS entries since they don't correspond to actual database tables.

## Parameters / Member Variables
- `*AH`: Archive handle containing the database connection and dump context
- `*te`: Table of Contents entry representing the table to be locked, containing namespace and tag information
## Dependencies
- Functions called/Symbols referenced:
  - [fmtQualifiedId](../f/fmtQualifiedId.md) (formats qualified table identifier)
  - [PQexec](../P/PQexec.md) (executes SQL command)
  - PGRES_COMMAND_OK (success status constant)
  - [TocEntry](../T/TocEntry.md) (table of contents entry structure)

- Called from (representative examples):
  - [WaitForCommands](../W/WaitForCommands.md) (parallel worker command processing)

## Notes and Other Information
- Uses NOWAIT semantics to prevent deadlocks in parallel dump operations
- Critical for maintaining data consistency during concurrent access
- Part of pg_dump's parallel processing infrastructure
- Terminates the entire backup process if lock cannot be acquired
- Skips processing for BLOB entries which don't require table locks

## Simplified Source

```c
static void lockTableForWorker(ArchiveHandle *AH, TocEntry *te) {
    // Skip locking for BLOBS - they don't correspond to actual tables
    if (strcmp(te->desc, "BLOBS") == 0) {
        return;
    }

    // Build qualified table name for locking
    const char *qualId = fmtQualifiedId(te->namespace, te->tag);
    PQExpBuffer query = createPQExpBuffer();

    // Use NOWAIT to prevent deadlocks in parallel operations
    appendPQExpBuffer(query, "LOCK TABLE %s IN ACCESS SHARE MODE NOWAIT", qualId);

    // Execute lock command
    PGresult *res = PQexec(AH->connection, query->data);

    // Check if lock was acquired successfully
    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        pg_fatal("could not obtain lock on relation \"%s\"\n"
                "This usually means that someone requested an ACCESS EXCLUSIVE lock "
                "on the table after the pg_dump parent process had gotten the "
                "initial ACCESS SHARE lock on the table.", qualId);
    }

    // Cleanup
    PQclear(res);
    destroyPQExpBuffer(query);
}
```