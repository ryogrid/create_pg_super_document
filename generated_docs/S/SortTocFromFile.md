# SortTocFromFile

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1548 - 1628

## Overview
Reads a TOC (Table of Contents) file to reorder archive entries according to a user-specified sequence and marks selected entries as wanted for restoration.

## Definition
```c
void SortTocFromFile(Archive *AHX)
```

## Detailed Description
This function processes a TOC file that contains a list of dump IDs specifying the desired restoration order. It reads the file line by line, validates each ID, finds the corresponding TOC entry, marks it as wanted, and moves it to the end of the TOC list to establish the specified order. Lines can contain comments (after ';') which are ignored. The function ensures that unwanted items remain at the front of the list, which is important for proper dependency handling in parallel restores.

## Parameters / Member Variables
- `AHX`: Archive handle cast from the generic Archive pointer

## Dependencies
- Functions called/Symbols referenced:
  - RestoreOptions
  - pg_malloc0
  - PG_BINARY_R
  - fopen
  - pg_get_line_buf
  - DumpId
  - TocEntry
  - pg_log_warning
  - getTocEntryByDumpId
  - _moveBefore
  - pg_free
- Called from (representative examples):
  - main (in pg_restore.c)

## Notes and Other Information
- Allocates and initializes the idWanted boolean array based on maxDumpId
- Supports comments in TOC files (text after ';' is ignored)
- Validates dump IDs to ensure they are positive, within range, and not duplicated
- Moves selected entries to the end of the list in the order they appear in the file
- Unwanted items remain at the front, which helps with dependency resolution in parallel restores
- Uses pg_get_line_buf for robust line reading with proper memory management
- Provides warnings for invalid lines but continues processing
- Fatal errors occur for file I/O problems or missing TOC entries