# mark_create_done

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4943-4957](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4943-L4957)

## Overview
This function sets the created flag on the DATA member that corresponds to a given TABLE member, indicating that the table structure has been successfully created during the restore process.

## Definition
static void mark_create_done(ArchiveHandle *AH, TocEntry *te)

## Detailed Description
The function serves as a state management utility in the PostgreSQL dump/restore process. When a TABLE TOC entry is successfully processed (typically after the table structure has been created), this function locates the corresponding TABLE DATA entry using the tableDataId mapping array and marks it as created by setting its created flag to true.

This mechanism is essential for coordinating the restore process, particularly in scenarios where table creation and data loading are handled as separate operations. By marking the DATA entry as created when the corresponding TABLE is processed, the restore system can track which table structures are ready to receive data, ensuring proper sequencing and preventing attempts to load data into non-existent tables.

## Parameters / Member Variables
- : Archive handle containing the dump metadata and TOC entries, including the tableDataId mapping array
- : The TABLE TOC entry that has been successfully processed and whose corresponding DATA entry should be marked as created

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md) (struct type)
- Called from (representative examples):
  - [restore_toc_entry](../r/restore_toc_entry.md)
  - [mark_restore_job_done](mark_restore_job_done.md)

## Notes and Other Information
- Only operates if there is a corresponding DATA entry (tableDataId mapping exists and is non-zero)
- The created flag is a boolean field in the TocEntry structure used for state tracking
- This function bridges the gap between table creation and data loading phases of the restore process
- Essential for maintaining consistency in complex restore scenarios, especially with parallel processing
- The tableDataId array provides the mapping between TABLE dump IDs and their corresponding TABLE DATA dump IDs
- Typically called after successful completion of table creation operations during restore