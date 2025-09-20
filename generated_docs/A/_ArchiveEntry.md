# _ArchiveEntry

## Location
[src/bin/pg_dump/pg_backup_custom.c:199-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L199-L221)

## Overview
_ArchiveEntry is a callback function invoked by the PostgreSQL archiver when the dumper creates a new Table of Contents (TOC) entry, responsible for setting up format-specific TOC data for the custom archive format.

## Definition

```c
static void
_ArchiveEntry(ArchiveHandle *AH, TocEntry *te)
```
## Detailed Description
This function serves as an optional callback that is called whenever a new TOC entry is created during the dump process. Its primary purpose is to initialize format-specific data structures associated with each TOC entry in the custom archive format.

The function creates a local TOC entry context (lclTocEntry) and initializes its data state based on whether the TOC entry has an associated data dumper function. This state tracking is crucial for managing data offset positions during the archive creation and restoration process.

The data state is set to either:
- K_OFFSET_POS_NOT_SET: When the entry has a data dumper (indicating data will be written)
- K_OFFSET_NO_DATA: When the entry has no data dumper (indicating no data associated with this entry)

## Parameters / Member Variables
- : Pointer to the ArchiveHandle structure containing archive context information
- : Pointer to the TocEntry structure representing the TOC entry being processed

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0 (memory allocation)
  - lclTocEntry (local TOC entry structure type)
  - K_OFFSET_POS_NOT_SET, K_OFFSET_NO_DATA (offset state constants)

- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (assigned as ArchiveEntryPtr function pointer)
  - Referenced by InitArchiveFmt_Directory (directory format also uses this pattern)

## Notes and Other Information
- This function is declared as static, meaning it's only accessible within the pg_backup_custom.c file
- The function is optional in the archive format interface but is implemented for proper state management in the custom format
- The allocated lclTocEntry context is attached to the TOC entry's formatData field for later use during data processing
- The data state tracking enables efficient seeking and positioning during archive restoration
- Memory allocated here should be properly freed when the TOC entry is destroyed (handled by other cleanup functions in the archive system)