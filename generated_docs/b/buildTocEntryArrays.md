# buildTocEntryArrays

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1966-2006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1966-L2006)

## Overview
Builds index arrays for efficient lookup of TOC (Table of Contents) entries by dump ID after all TOC items have been created or read.

## Definition
```c
static void buildTocEntryArrays(ArchiveHandle *AH)
```

## Detailed Description
This function creates two critical index arrays that enable fast lookup of TOC entries by dump ID rather than requiring linear traversal of the linked list. It should only be invoked after all TOC items have been created or read from an archive.

The function creates two arrays:
1. `tocsByDumpId`: Direct index from dump ID to TOC entry pointer
2. `tableDataId`: Maps table dump IDs to their corresponding TABLE DATA dump IDs

The arrays are indexed by dump ID with entry zero unused. Array entries only extend up to `maxDumpId`, so bounds checking is required when accessing entries that might reference dump IDs from partial dumps.

For TABLE DATA entries, the function establishes a reverse mapping by examining the dependency relationship, knowing that TABLE DATA items have exactly one dependency (the corresponding TABLE item).

## Parameters / Member Variables
- `AH`: Archive handle containing the TOC list and where the index arrays will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](../p/pg_malloc0.md) (memory allocation)
  - [pg_fatal](../p/pg_fatal.md) (error reporting)
  - DumpId (type)
  - [TocEntry](../T/TocEntry.md) (struct type)
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md) (during archive restoration process)
  - [getTocEntryByDumpId](../g/getTocEntryByDumpId.md) (ensures arrays are built before lookup)

## Notes and Other Information
- This is a static function only accessible within pg_backup_archiver.c
- The function includes paranoia checks to ensure dump IDs are within expected bounds
- Array bounds checking is critical when accessing these arrays since dependency dump IDs might reference items beyond maxDumpId in partial dumps
- The tableDataId mapping is computed by reversing the dependency relationship of TABLE DATA items
- Memory is allocated using pg_malloc0 to ensure zero-initialization
- The function assumes the TOC list is properly formed with a circular linked list structure