# restore_toc_entries_prefork

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4154-4276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4154-L4276)

## Overview
Main engine for the first phase of parallel restore, processing PRE_DATA TOC entries that can be handled in the main restore pass.

## Definition

```c
structure.  When we can no longer
 * make any entries ready to process, we exit.  Normally, there will be
 * nothing left to do;
```
## Detailed Description
This function implements the first phase of PostgreSQL's parallel restore process. It processes all SECTION_PRE_DATA TOC (Table of Contents) entries that are allowed to be processed in the RESTORE_PASS_MAIN pass, which includes most PRE_DATA items except ACLs. The function operates in a single connection in the parent process for efficiency, since pre-data operations benefit less from parallelization and older pg_dump versions had incomplete dependency information for these items.

The function iterates through all TOC entries, identifying those that can be processed immediately (mainly PRE_DATA items) and deferring others to the pending_list for later phases. It ensures proper dependency handling and maintains transactional integrity by committing transactions and cleaning up connection state before parallel processing begins.

## Parameters / Member Variables
- `AH`: ArchiveHandle containing the restore context, TOC entries, and connection information
- `pending_list`: TocEntry list where items that cannot be processed now are queued for later phases

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_debug (logging function for debug messages)
  - [fix_dependencies](../f/fix_dependencies.md) (adjusts dependency information before processing)
  - [_tocEntryRestorePass](../t/_tocEntryRestorePass.md) (determines which restore pass an entry belongs to)
  - pg_log_info (logging function for informational messages)
  - [restore_toc_entry](restore_toc_entry.md) (processes individual TOC entry restoration)
  - [reduce_dependencies](reduce_dependencies.md) (updates dependency counts after item completion)
  - [pending_list_append](../p/pending_list_append.md) (adds items to the pending list for later processing)
  - [CommitTransaction](../C/CommitTransaction.md) (commits current transaction if in transaction-size mode)
  - [DisconnectDatabase](../D/DisconnectDatabase.md) (closes parent connection to prepare for parallel steps)
  - SECTION_PRE_DATA, SECTION_DATA, SECTION_POST_DATA (section type constants)
  - RESTORE_PASS_MAIN (restore pass constant)
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md) (main restore orchestration function)

## Notes and Other Information
- This function is part of a three-phase parallel restore system
- Processes items sequentially in the parent process for better performance with pre-data items
- Handles backward compatibility with pre-9.2 pg_dump versions that had incomplete dependencies
- Maintains proper ordering despite potential disruption from list files (SortTocFromFile)
- Commits transactions in transaction-size mode to ensure child workers can see created objects
- Cleans up connection state (currUser, currSchema, currTablespace, currTableAm) after disconnection
- Uses a 'skipped_some' flag to handle SECTION_NONE items (like comments) that depend on ordering
- Does not filter out non-restorable entries initially, as they may participate in dependency chains

## Simplified Source

```c
static void
restore_toc_entries_prefork(ArchiveHandle *AH, TocEntry *pending_list)
{
    bool skipped_some;
    TocEntry *next_work_item;

    pg_log_debug("entering restore_toc_entries_prefork");

    // Adjust dependency information before processing
    fix_dependencies(AH);

    // Process PRE_DATA items in single connection for efficiency
    // Items that can't be processed now go to pending_list
    AH->restorePass = RESTORE_PASS_MAIN;
    skipped_some = false;

    for (next_work_item = AH->toc->next; next_work_item != AH->toc; next_work_item = next_work_item->next)
    {
        bool do_now = true;

        // Check if item should be processed now
        if (next_work_item->section != SECTION_PRE_DATA)
        {
            if (next_work_item->section == SECTION_DATA ||
                next_work_item->section == SECTION_POST_DATA)
            {
                // DATA and POST_DATA items deferred to later phases
                do_now = false;
                skipped_some = true;
            }
            else
            {
                // SECTION_NONE items (comments) can be processed if we haven't skipped anything
                // Once we've skipped items, comments must wait to check dependencies
                if (skipped_some)
                    do_now = false;
            }
        }

        // Skip items that need to be forced into later passes
        if (_tocEntryRestorePass(next_work_item) != RESTORE_PASS_MAIN)
            do_now = false;

        if (do_now)
        {
            // Process the item immediately
            pg_log_info("processing item %d %s %s",
                        next_work_item->dumpId,
                        next_work_item->desc, next_work_item->tag);

            restore_toc_entry(AH, next_work_item, false);

            // Update dependencies but don't move anything to ready_heap yet
            reduce_dependencies(AH, next_work_item, NULL);
        }
        else
        {
            // Defer to later phases
            pending_list_append(pending_list, next_work_item);
        }
    }

    // Commit transaction in transaction-size mode so child workers can see objects
    if (AH->public.ropt->txn_size > 0)
        CommitTransaction(&AH->public);

    // Close parent connection to prepare for parallel steps
    // This ensures we don't exceed the specified parallel connection limit
    DisconnectDatabase(&AH->public);

    // Clean up transient state from the old connection
    free(AH->currUser);
    AH->currUser = NULL;
    free(AH->currSchema);
    AH->currSchema = NULL;
    free(AH->currTablespace);
    AH->currTablespace = NULL;
    free(AH->currTableAm);
    AH->currTableAm = NULL;
}
```