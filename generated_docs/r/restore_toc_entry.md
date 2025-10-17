# restore_toc_entry

## Location
[src/bin/pg_dump/pg_backup_archiver.c:834-1089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L834-L1089)

## Overview
The restore_toc_entry function is the core function responsible for restoring a single TOC (Table of Contents) item during PostgreSQL backup restoration, supporting both parallel and non-parallel restore modes.

## Definition

```c
static int
restore_toc_entry(ArchiveHandle *AH, TocEntry *te, bool is_parallel)
```
## Detailed Description
This function handles the complete restoration of a single TOC entry, which can include both schema (DDL) and data components. It manages transaction boundaries, error handling, parallel processing coordination, and various restoration optimizations. The function processes two main phases:

1. **Schema Phase (REQ_SCHEMA)**: Creates database objects like tables, indexes, functions, etc. For TABLE objects, it tracks creation success/failure to coordinate with data loading.

2. **Data Phase (REQ_DATA)**: Loads actual data into tables, handles BLOB data, and executes data-related statements. It includes optimizations like TRUNCATE before COPY in parallel mode for better performance.

The function includes special handling for database objects, trigger management, transaction size limits, and coordination between parallel worker processes and the parent process through return status codes.

## Parameters / Member Variables
- `AH`: Archive handle containing restoration context and connection information
- `te`: TOC entry to be restored, containing object definition, data, and metadata
- `is_parallel`: Boolean flag indicating if running in a parallel worker process

## Dependencies
- Functions called/Symbols referenced:
  - [CommitTransaction](../C/CommitTransaction.md)
  - [StartTransaction](../S/StartTransaction.md)
  - [inhibit_data_for_failed_table](../i/inhibit_data_for_failed_table.md)
  - [mark_create_done](../m/mark_create_done.md)
  - [_disableTriggersIfNecessary](../d/_disableTriggersIfNecessary.md)
  - [_enableTriggersIfNecessary](../e/_enableTriggersIfNecessary.md)
  - [is_load_via_partition_root](../i/is_load_via_partition_root.md)
  - [_printTocEntry](../p/_printTocEntry.md)
  - [_reconnectToDB](_reconnectToDB.md)
  - [_becomeOwner](../b/_becomeOwner.md)
  - [_selectOutputSchema](../s/_selectOutputSchema.md)
  - pg_log_info
  - pg_log_warning
  - [ahprintf](../a/ahprintf.md)
  - [EndDBCopyMode](../E/EndDBCopyMode.md)
  - [RestoringToDB](../R/RestoringToDB.md)
  - [fmtQualifiedId](../f/fmtQualifiedId.md)
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md)
  - [parallel_restore](../p/parallel_restore.md)
  - [restore_toc_entries_prefork](restore_toc_entries_prefork.md)
  - [restore_toc_entries_postfork](restore_toc_entries_postfork.md)

## Notes and Other Information
- Returns status codes: WORKER_OK (normal), WORKER_CREATE_DONE (table created successfully), WORKER_INHIBIT_DATA (table creation failed), or WORKER_IGNORED_ERRORS (errors occurred)
- Supports transaction-size mode for batching operations into transactions
- Handles special cases for DATABASE and BLOB objects with different processing logic
- Includes performance optimization for parallel restore using TRUNCATE before COPY when safe
- Coordinates with parallel parent process through return status codes rather than direct state modification
- Manages trigger disable/enable around data loading for performance
- Tracks current TOC entry in AH->currentTE for error reporting context

## Simplified Source

```c
static int
restore_toc_entry(ArchiveHandle *AH, TocEntry *te, bool is_parallel)
{
    RestoreOptions *ropt = AH->public.ropt;
    int status = WORKER_OK;
    int reqs;
    bool defnDumped;

    AH->currentTE = te;

    // Show any warnings from the original dump
    if (!ropt->suppressDumpWarnings && strcmp(te->desc, "WARNING") == 0)
    {
        if (!ropt->dataOnly && te->defn != NULL && strlen(te->defn) != 0)
            pg_log_warning("warning from original dump file: %s", te->defn);
    }

    reqs = te->reqs;
    defnDumped = false;

    // Process schema component (DDL like CREATE TABLE, etc.)
    if ((reqs & REQ_SCHEMA) != 0)
    {
        bool object_is_db = false;

        // Special handling for DATABASE objects
        if (strcmp(te->desc, "DATABASE") == 0 ||
            strcmp(te->desc, "DATABASE PROPERTIES") == 0)
        {
            object_is_db = true;
            // Must exit transaction for database operations
            if (ropt->txn_size > 0)
            {
                if (AH->connection)
                    CommitTransaction(&AH->public);
                else
                    ahprintf(AH, "COMMIT;\n\n");
            }
        }

        // Log what we're creating
        if (te->namespace)
            pg_log_info("creating %s \"%s.%s\"", te->desc, te->namespace, te->tag);
        else
            pg_log_info("creating %s \"%s\"", te->desc, te->tag);

        // Output the DDL statements
        _printTocEntry(AH, te, false);
        defnDumped = true;

        // Special handling for TABLE creation results
        if (strcmp(te->desc, "TABLE") == 0)
        {
            if (AH->lastErrorTE == te)
            {
                // Table creation failed
                if (ropt->noDataForFailedTables)
                {
                    if (is_parallel)
                        status = WORKER_INHIBIT_DATA;
                    else
                        inhibit_data_for_failed_table(AH, te);
                }
            }
            else
            {
                // Table creation succeeded
                if (is_parallel)
                    status = WORKER_CREATE_DONE;
                else
                    mark_create_done(AH, te);
            }
        }

        // Reconnect to new database if we created one
        if (object_is_db)
        {
            pg_log_info("connecting to new database \"%s\"", te->tag);
            _reconnectToDB(AH, te->tag);
        }
    }

    // Process data component (actual table data, etc.)
    if ((reqs & REQ_DATA) != 0)
    {
        if (te->hadDumper)
        {
            // This entry has actual data to restore
            if (AH->PrintTocDataPtr != NULL)
            {
                _printTocEntry(AH, te, true);

                if (strcmp(te->desc, "BLOBS") == 0 ||
                    strcmp(te->desc, "BLOB COMMENTS") == 0)
                {
                    // Handle BLOB data specially
                    pg_log_info("processing %s", te->desc);
                    _selectOutputSchema(AH, "pg_catalog");

                    if (strcmp(te->desc, "BLOB COMMENTS") == 0)
                        AH->outputKind = OUTPUT_OTHERDATA;

                    AH->PrintTocDataPtr(AH, te);
                    AH->outputKind = OUTPUT_SQLCMDS;
                }
                else
                {
                    // Handle regular table data
                    bool use_truncate;

                    _disableTriggersIfNecessary(AH, te);
                    _becomeOwner(AH, te);
                    _selectOutputSchema(AH, te->namespace);

                    pg_log_info("processing data for table \"%s.%s\"",
                                te->namespace, te->tag);

                    // Use TRUNCATE optimization in parallel mode if safe
                    use_truncate = is_parallel && te->created &&
                                   !is_load_via_partition_root(te);

                    if (use_truncate)
                    {
                        StartTransaction(&AH->public);
                        ahprintf(AH, "TRUNCATE TABLE ONLY %s;\n\n",
                                 fmtQualifiedId(te->namespace, te->tag));
                    }

                    // Output COPY statement if available
                    if (te->copyStmt && strlen(te->copyStmt) > 0)
                    {
                        ahprintf(AH, "%s", te->copyStmt);
                        AH->outputKind = OUTPUT_COPYDATA;
                    }
                    else
                        AH->outputKind = OUTPUT_OTHERDATA;

                    // Load the actual data
                    AH->PrintTocDataPtr(AH, te);

                    // Terminate COPY if needed
                    if (AH->outputKind == OUTPUT_COPYDATA && RestoringToDB(AH))
                        EndDBCopyMode(&AH->public, te->tag);
                    AH->outputKind = OUTPUT_SQLCMDS;

                    // Close transaction started for TRUNCATE
                    if (use_truncate)
                        CommitTransaction(&AH->public);

                    _enableTriggersIfNecessary(AH, te);
                }
            }
        }
        else if (!defnDumped)
        {
            // No actual data dumper, but may have statements to execute
            pg_log_info("executing %s %s", te->desc, te->tag);
            _printTocEntry(AH, te, false);
        }
    }

    // Handle transaction-size batching
    if ((reqs & (REQ_SCHEMA | REQ_DATA)) != 0 && ropt->txn_size > 0)
    {
        if (++AH->txnCount >= ropt->txn_size)
        {
            if (AH->connection)
            {
                CommitTransaction(&AH->public);
                StartTransaction(&AH->public);
            }
            else
                ahprintf(AH, "COMMIT;\nBEGIN;\n\n");
            AH->txnCount = 0;
        }
    }

    // Check for errors and update status
    if (AH->public.n_errors > 0 && status == WORKER_OK)
        status = WORKER_IGNORED_ERRORS;

    return status;
}
```