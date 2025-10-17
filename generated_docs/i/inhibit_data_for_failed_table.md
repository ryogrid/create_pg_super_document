# inhibit_data_for_failed_table

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4958-4977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4958-L4977)

## Overview
This function marks the DATA member corresponding to a failed TABLE member as not wanted, preventing data restoration attempts for tables that could not be created.

## Definition
static void inhibit_data_for_failed_table(ArchiveHandle *AH, TocEntry *te)

## Detailed Description
The function serves as an error recovery mechanism in the PostgreSQL dump/restore process. When a TABLE TOC entry fails to be created during the restore operation, this function locates the corresponding TABLE DATA entry using the tableDataId mapping array and marks it as not wanted by setting its reqs field to 0. This prevents the restore process from attempting to load data into a table that doesn't exist or wasn't created successfully.

The function also logs an informational message indicating that the table could not be created and its data will not be restored. This provides clear feedback to users about which tables encountered problems during the restore process, helping with troubleshooting and understanding the final state of the restored database.

## Parameters / Member Variables
- : Archive handle containing the dump metadata and TOC entries, including the tableDataId mapping array
- : The TABLE TOC entry that failed to be created and whose corresponding DATA entry should be marked as not wanted

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md) (struct type)
  - pg_log_info (logging function for informational messages)
- Called from (representative examples):
  - [restore_toc_entry](../r/restore_toc_entry.md)
  - [mark_restore_job_done](../m/mark_restore_job_done.md)

## Notes and Other Information
- Only operates if there is a corresponding DATA entry (tableDataId mapping exists and is non-zero)
- The reqs field in TocEntry indicates whether an entry should be restored (0 means do not restore)
- Essential for preventing cascading errors when table creation fails
- Provides user-friendly logging to inform about skipped data restoration
- Works in conjunction with the tableDataId mapping to coordinate table and data operations
- Helps maintain restore process stability by gracefully handling table creation failures
- The tag field in the TocEntry is used in the log message to identify the failed table by name

## Simplified Source

```c
static void inhibit_data_for_failed_table(ArchiveHandle *AH, TocEntry *te) {
    // Log that table creation failed and data won't be restored
    pg_log_info("table \"%s\" could not be created, will not restore its data",
                te->tag);

    // Find corresponding DATA entry and mark it as not wanted
    if (AH->tableDataId[te->dumpId] != 0) {
        TocEntry *data_entry = AH->tocsByDumpId[AH->tableDataId[te->dumpId]];
        data_entry->reqs = 0;  // Mark as not wanted for restoration
    }
}
```