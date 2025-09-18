# ProcessArchiveRestoreOptions

## Location
src/bin/pg_dump/pg_backup_archiver.c: 279 - 333

## Overview
Processes and validates archive restore options, determining which TOC entries should be processed and ensuring archive section ordering integrity.

## Definition
```c
void ProcessArchiveRestoreOptions(Archive *AHX)
```

## Detailed Description
The ProcessArchiveRestoreOptions function performs critical preprocessing of archive restore operations. It iterates through all Table of Contents (TOC) entries in the archive to determine which entries should be dumped or restored based on the configured options. The function also validates the logical ordering of archive sections (PRE_DATA, DATA, POST_DATA) to ensure archive integrity.

During archive writing operations, the function enforces strict section ordering and issues warnings for out-of-order entries. For archive reading operations, it's more lenient to handle potentially buggy archives from older pg_dump versions. The function uses the _tocEntryRequired helper to evaluate whether each TOC entry meets the selection criteria based on the current restore options.

If strict name checking is enabled, the function calls StrictNamesCheck to validate that all specified objects exist in the archive.

## Parameters / Member Variables
- `AHX`: Pointer to the Archive structure to process

## Dependencies
- Functions called/Symbols referenced:
  - _tocEntryRequired
  - StrictNamesCheck
  - pg_log_warning
  - pg_fatal
  - RestoreOptions
  - TocEntry
  - teSection
  - SECTION_PRE_DATA
  - SECTION_DATA
  - SECTION_POST_DATA
  - SECTION_NONE
  - archModeRead
- Called from (representative examples):
  - main (in pg_dump.c)
  - main (in pg_restore.c)

## Notes and Other Information
- This is a public function in the pg_dump/pg_restore architecture
- The function validates archive section ordering during write operations but is lenient during reads
- TOC entry requirements are determined by calling _tocEntryRequired for each entry
- Section validation helps ensure logical consistency of dump operations
- The function handles both dump and restore scenarios with different validation strictness
- Strict name checking is optional and controlled by restore options
- Critical for determining the scope of restore operations based on user-specified criteria