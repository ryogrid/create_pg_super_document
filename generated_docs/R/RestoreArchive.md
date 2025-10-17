# RestoreArchive

## Location
[src/bin/pg_dump/pg_backup_archiver.c:334-833](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L334-L833)

## Overview
Performs the complete restoration process of a PostgreSQL dump archive, handling both serial and parallel restore modes with comprehensive transaction management and error handling.

## Definition
```c
void RestoreArchive(Archive *AHX)
```

## Detailed Description
The RestoreArchive function is the main entry point for restoring a PostgreSQL dump archive. It orchestrates the entire restoration process through multiple stages: initialization, processing, and finalization. The function supports both serial and parallel restore modes, with extensive validation and error checking throughout.

The restoration process includes several key phases:
1. **Initialization**: Validates parallel restore compatibility, checks compression support, and establishes database connections if needed
2. **Schema Analysis**: Determines if the restore is data-only based on available TOC entries
3. **Output Setup**: Configures output files and compression as specified
4. **Drop Phase**: Optionally drops existing objects in reverse dependency order
5. **Restore Phase**: Processes TOC entries in appropriate order (serial mode uses three passes: main, ACL, post-ACL; parallel mode uses worker processes)
6. **Finalization**: Commits transactions, closes connections, and performs cleanup

The function handles various restore options including single transactions, transaction batching, parallel processing, and conditional object creation/dropping.

## Parameters / Member Variables
- `AHX`: Pointer to the Archive structure containing the dump to restore

## Dependencies
- Functions called/Symbols referenced:
  - [buildTocEntryArrays](../b/buildTocEntryArrays.md)
  - [ConnectDatabase](../C/ConnectDatabase.md)
  - [DisconnectDatabase](../D/DisconnectDatabase.md)
  - [SaveOutput](../S/SaveOutput.md)
  - [SetOutput](../S/SetOutput.md)
  - [RestoreOutput](RestoreOutput.md)
  - [StartTransaction](../S/StartTransaction.md)
  - [CommitTransaction](../C/CommitTransaction.md)
  - [_doSetFixedOutputState](../d/_doSetFixedOutputState.md)
  - [_becomeOwner](../b/_becomeOwner.md)
  - [_selectOutputSchema](../s/_selectOutputSchema.md)
  - [restore_toc_entry](../r/restore_toc_entry.md)
  - [restore_toc_entries_prefork](../r/restore_toc_entries_prefork.md)
  - [restore_toc_entries_parallel](../r/restore_toc_entries_parallel.md)
  - [restore_toc_entries_postfork](../r/restore_toc_entries_postfork.md)
  - [ParallelBackupStart](../P/ParallelBackupStart.md)
  - [ParallelBackupEnd](../P/ParallelBackupEnd.md)
  - [IssueCommandPerBlob](../I/IssueCommandPerBlob.md)
  - [DropLOIfExists](../D/DropLOIfExists.md)
  - [supports_compression](../s/supports_compression.md)
  - [dumpTimestamp](../d/dumpTimestamp.md)
  - [ahprintf](../a/ahprintf.md)
  - pg_log_info
  - pg_log_warning
  - [pg_fatal](../p/pg_fatal.md)
  - And many constants and enums for stages, sections, and requirements
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c)
  - [main](../m/main.md) (in pg_restore.c)
  - [_CloseArchive](../C/_CloseArchive.md) (in pg_backup_tar.c)

## Notes and Other Information
- This is a public function and the primary interface for archive restoration
- Supports both serial and parallel restore modes with different processing strategies
- Handles comprehensive transaction management including single transactions and batched commits
- Provides extensive validation for parallel restore compatibility and compression support
- Manages object dropping with proper dependency ordering and IF EXISTS clause injection
- Includes sophisticated error handling and logging throughout the process
- The function spans over 500 lines, making it one of the most complex functions in the pg_dump architecture
- Critical for both standalone pg_restore operations and integrated dump-restore workflows
- Supports various output formats and compression algorithms
- Handles special cases for large objects, database properties, and constraint objects

## Simplified Source

```c
void
RestoreArchive(Archive *AHX)
{
    ArchiveHandle *AH = (ArchiveHandle *) AHX;
    RestoreOptions *ropt = AH->public.ropt;
    bool parallel_mode;
    TocEntry *te;

    AH->stage = STAGE_INITIALIZING;

    // Check if parallel restore is requested and supported
    parallel_mode = (AH->public.numWorkers > 1 && ropt->useDB);
    if (parallel_mode)
    {
        // Validate parallel restore compatibility
        if (AH->ClonePtr == NULL || AH->ReopenPtr == NULL)
            pg_fatal("parallel restore is not supported with this archive file format");
        if (AH->version < K_VERS_1_8)
            pg_fatal("parallel restore is not supported with archives made by pre-8.0 pg_dump");

        // Test that we can reopen the input file
        AH->ReopenPtr(AH);
    }

    // Check compression support for data entries
    if (AH->PrintTocDataPtr != NULL)
    {
        for (te = AH->toc->next; te != AH->toc; te = te->next)
        {
            if (te->hadDumper && (te->reqs & REQ_DATA) != 0)
            {
                char *errmsg = supports_compression(AH->compression_spec);
                if (errmsg)
                    pg_fatal("cannot restore from compressed archive (%s)", errmsg);
                break;
            }
        }
    }

    // Prepare TOC entry arrays if not already done
    if (AH->tocsByDumpId == NULL)
        buildTocEntryArrays(AH);

    // Connect to database if needed
    if (ropt->useDB)
    {
        pg_log_info("connecting to database for restore");
        ConnectDatabase(AHX, &ropt->cparams, false);
        AH->noTocComments = 1;  // Don't send comments to DB
    }

    // Check if this is an implied data-only restore
    if (!ropt->dataOnly)
    {
        int impliedDataOnly = 1;
        for (te = AH->toc->next; te != AH->toc; te = te->next)
        {
            if ((te->reqs & REQ_SCHEMA) != 0)
            {
                impliedDataOnly = 0;
                break;
            }
        }
        if (impliedDataOnly)
        {
            ropt->dataOnly = impliedDataOnly;
            pg_log_info("implied data-only restore");
        }
    }

    // Setup output file and write header
    CompressFileHandle *sav = SaveOutput(AH);
    if (ropt->filename || ropt->compression_spec.algorithm != PG_COMPRESSION_NONE)
        SetOutput(AH, ropt->filename, ropt->compression_spec);

    ahprintf(AH, "--\n-- PostgreSQL database dump\n--\n\n");

    // Start transaction if requested
    if (ropt->single_txn)
    {
        if (AH->connection)
            StartTransaction(AHX);
        else
            ahprintf(AH, "BEGIN;\n\n");
    }

    _doSetFixedOutputState(AH);
    AH->stage = STAGE_PROCESSING;

    // Drop phase: remove existing objects if requested
    if (ropt->dropSchema)
    {
        for (te = AH->toc->prev; te != AH->toc; te = te->prev)
        {
            if (((te->reqs & (REQ_SCHEMA | REQ_DATA)) != 0) && te->dropStmt)
            {
                pg_log_info("dropping %s %s", te->desc, te->tag);
                _becomeOwner(AH, te);
                _selectOutputSchema(AH, te->namespace);

                // Emit DROP command with IF EXISTS if requested
                if (*te->dropStmt != '\0')
                    ahprintf(AH, "%s", te->dropStmt);  // Simplified drop handling
            }
        }

        // Reset schema tracking after drops
        free(AH->currSchema);
        AH->currSchema = NULL;
    }

    // Main restore phase
    if (parallel_mode)
    {
        // Parallel restore using worker processes
        ParallelState *pstate;
        TocEntry pending_list;

        if (AH->PrepParallelRestorePtr)
            AH->PrepParallelRestorePtr(AH);

        pending_list_header_init(&pending_list);
        restore_toc_entries_prefork(AH, &pending_list);

        pstate = ParallelBackupStart(AH);
        restore_toc_entries_parallel(AH, pstate, &pending_list);
        ParallelBackupEnd(AH, pstate);

        restore_toc_entries_postfork(AH, &pending_list);
    }
    else
    {
        // Serial restore with three passes: main, ACL, post-ACL
        bool haveACL = false;
        bool havePostACL = false;

        // First pass: main objects
        for (te = AH->toc->next; te != AH->toc; te = te->next)
        {
            if ((te->reqs & (REQ_SCHEMA | REQ_DATA)) == 0)
                continue;

            switch (_tocEntryRestorePass(te))
            {
                case RESTORE_PASS_MAIN:
                    restore_toc_entry(AH, te, false);
                    break;
                case RESTORE_PASS_ACL:
                    haveACL = true;
                    break;
                case RESTORE_PASS_POST_ACL:
                    havePostACL = true;
                    break;
            }
        }

        // Second pass: ACL objects
        if (haveACL)
        {
            for (te = AH->toc->next; te != AH->toc; te = te->next)
            {
                if ((te->reqs & (REQ_SCHEMA | REQ_DATA)) != 0 &&
                    _tocEntryRestorePass(te) == RESTORE_PASS_ACL)
                    restore_toc_entry(AH, te, false);
            }
        }

        // Third pass: post-ACL objects
        if (havePostACL)
        {
            for (te = AH->toc->next; te != AH->toc; te = te->next)
            {
                if ((te->reqs & (REQ_SCHEMA | REQ_DATA)) != 0 &&
                    _tocEntryRestorePass(te) == RESTORE_PASS_POST_ACL)
                    restore_toc_entry(AH, te, false);
            }
        }
    }

    // Commit final transaction
    if (ropt->single_txn || ropt->txn_size > 0)
    {
        if (AH->connection)
            CommitTransaction(AHX);
        else
            ahprintf(AH, "COMMIT;\n\n");
    }

    // Write completion message and cleanup
    ahprintf(AH, "--\n-- PostgreSQL database dump complete\n--\n\n");

    AH->stage = STAGE_FINALIZING;

    if (ropt->filename || ropt->compression_spec.algorithm != PG_COMPRESSION_NONE)
        RestoreOutput(AH, sav);

    if (ropt->useDB)
        DisconnectDatabase(&AH->public);
}
```