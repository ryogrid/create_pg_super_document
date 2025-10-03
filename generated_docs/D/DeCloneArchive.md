# DeCloneArchive

## Location
[src/bin/pg_dump/pg_backup_archiver.c:5042-5062](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L5042-L5062)

## Overview
Releases and cleans up clone-local storage and state for an ArchiveHandle that was previously created by CloneArchive, ensuring proper memory management in parallel restoration processes.

## Definition

```c
void
DeCloneArchive(ArchiveHandle *AH)
```
## Detailed Description
DeCloneArchive performs cleanup operations for an ArchiveHandle clone created by CloneArchive. The function ensures that the database connection has been properly closed, delegates format-specific cleanup to the DeClonePtr function pointer, destroys any SQL parsing buffers, and frees all connection-local state including user, schema, tablespace, table access method, and password information. Finally, it frees the ArchiveHandle structure itself.

## Parameters / Member Variables
- `*AH`: The cloned ArchiveHandle to be destroyed and cleaned up
## Dependencies
- Functions called/Symbols referenced:
  - No direct function calls (uses free() and structure field access)
- Called from (representative examples):
  - [RunWorker](../R/RunWorker.md) (from src/bin/pg_dump/parallel.c:866)
  - ARCHIVE_OPTS (from src/bin/pg_dump/pg_backup_archiver.h:422)

## Notes and Other Information
- Must be called only after the database connection has been closed (enforced by Assert)
- Format-specific cleanup is performed through the DeClonePtr function pointer
- Frees SQL parsing state including curCmd buffer if allocated
- Cleans up all connection-local strings (currUser, currSchema, currTablespace, currTableAm, savedPassword)
- Should be called as the final step when a parallel worker thread completes its work
- Paired with CloneArchive for proper resource management in parallel operations
- The function assumes the caller has already properly closed any database connections