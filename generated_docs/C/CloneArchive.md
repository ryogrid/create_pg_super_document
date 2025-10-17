# CloneArchive

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4978-5041](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4978-L5041)

## Overview
Creates a clone of an ArchiveHandle structure for use in parallel restoration processes, ensuring that each worker thread has its own isolated copy with independent connection and state management.

## Definition

```c
ArchiveHandle *
CloneArchive(ArchiveHandle *AH)
```
## Detailed Description
CloneArchive creates a deep clone of an ArchiveHandle for parallel processing in pg_dump/pg_restore operations. The function performs a "flat" copy of the main structure and then selectively clones or resets specific fields to ensure thread safety. Each clone gets its own database connection, error tracking, and transaction management settings. The cloned archive is immediately connected to the database using the same connection parameters as the original, and format-specific cloning is performed through the ClonePtr function pointer.

## Parameters / Member Variables
- `*AH`: The source ArchiveHandle to be cloned
## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
  - [RestoreOptions](../R/RestoreOptions.md)
  - [ConnectDatabase](ConnectDatabase.md)
  - archModeRead
  - [_doSetFixedOutputState](../d/_doSetFixedOutputState.md)
- Called from (representative examples):
  - [RunWorker](../R/RunWorker.md) (from src/bin/pg_dump/parallel.c:846)
  - ARCHIVE_OPTS (from src/bin/pg_dump/pg_backup_archiver.h:421)

## Notes and Other Information
- The clone gets its own database connection to avoid conflicts between parallel workers
- Connection state fields (connection, connCancel, currUser, etc.) are reset to NULL and re-established
- Transaction size is set to 0 for clones to ensure immediate visibility of results to other workers
- savedPassword is duplicated if present to allow independent connection management
- Error counting is reset to 0 for each clone
- Format-specific cloning is delegated to the ClonePtr function pointer
- Used primarily in parallel restoration to ensure each worker thread operates independently

## Simplified Source

```c
ArchiveHandle *CloneArchive(ArchiveHandle *AH) {
    // Create flat copy of the main structure
    ArchiveHandle *clone = (ArchiveHandle *) pg_malloc(sizeof(ArchiveHandle));
    memcpy(clone, AH, sizeof(ArchiveHandle));

    // Clone RestoreOptions for independent modification
    clone->public.ropt = (RestoreOptions *) pg_malloc(sizeof(RestoreOptions));
    memcpy(clone->public.ropt, AH->public.ropt, sizeof(RestoreOptions));

    // Reset connection-related fields for independent connections
    memset(&(clone->sqlparse), 0, sizeof(clone->sqlparse));
    clone->connection = NULL;
    clone->connCancel = NULL;
    clone->currUser = NULL;
    clone->currSchema = NULL;
    clone->currTableAm = NULL;
    clone->currTablespace = NULL;

    // Duplicate password if present
    if (clone->savedPassword)
        clone->savedPassword = pg_strdup(clone->savedPassword);

    // Reset clone-specific state
    clone->public.n_errors = 0;
    clone->lo_buf = NULL;
    clone->public.ropt->txn_size = 0;  // Immediate commits for parallel work

    // Establish database connection for this clone
    ConnectDatabase((Archive *) clone, &clone->public.ropt->cparams, true);

    // Set up fixed state for read mode
    if (AH->mode == archModeRead)
        _doSetFixedOutputState(clone);

    // Let format-specific code perform additional cloning
    clone->ClonePtr(clone);

    Assert(clone->connection != NULL);
    return clone;
}
```