# getTocEntryByDumpId

## Location
src/bin/pg_dump/pg_backup_archiver.c: 2007 - 2019

## Overview
Retrieves a TOC (Table of Contents) entry by its dump ID, providing efficient O(1) lookup after building index arrays.

## Definition
```c
TocEntry *getTocEntryByDumpId(ArchiveHandle *AH, DumpId id)
```

## Detailed Description
This function provides fast access to TOC entries by dump ID using an index array. It ensures the index arrays are built if they haven't been created yet by calling `buildTocEntryArrays`. The function performs bounds checking to ensure the requested dump ID is within the valid range (1 to maxDumpId) before attempting array access.

The function is designed to be safe and efficient:
- Lazy initialization: builds index arrays only when first needed
- Bounds checking: validates dump ID before array access
- Null-safe: returns NULL for invalid or out-of-bounds dump IDs

This is a critical function for dependency resolution, parallel processing coordination, and general TOC entry lookup throughout the dump/restore process.

## Parameters / Member Variables
- `AH`: Archive handle containing the TOC list and index arrays
- `id`: The dump ID of the desired TOC entry

## Dependencies
- Functions called/Symbols referenced:
  - buildTocEntryArrays (ensures index arrays are available)
  - DumpId (type)
- Called from (representative examples):
  - parseWorkerCommand (parallel processing coordination)
  - SortTocFromFile (TOC reordering operations)  
  - TocIDRequired (dependency checking)
  - _tocEntryRequired (dependency resolution)
  - _PrintTocData (custom format operations)
  - IssueACLPerBlob (ACL processing)

## Notes and Other Information
- Returns NULL for invalid dump IDs (≤ 0 or > maxDumpId)
- Performs lazy initialization of index arrays for efficiency
- This function is thread-safe as long as the archive handle is not modified concurrently
- Essential for efficient dependency traversal and parallel processing coordination
- The function assumes that dump IDs are sequential and start from 1
- Used extensively throughout the dump/restore process for fast TOC entry lookup