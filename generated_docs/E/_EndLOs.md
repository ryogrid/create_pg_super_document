# _EndLOs

## Location
src/bin/pg_dump/pg_backup_custom.c: 406 - 415

## Overview
This function is called by the PostgreSQL archiver when finishing the saving of all BLOB (large object) data, marking the end of the large objects section in the archive.

## Definition
```c
static void _EndLOs(ArchiveHandle *AH, TocEntry *te)
```

## Detailed Description
_EndLOs is a callback function specific to the custom archive format that handles the finalization of all large object (BLOB) dump operations. Unlike _EndLO which handles individual large objects, this function marks the completion of the entire large objects section in the archive. It writes a fake zero OID (Object Identifier) to the archive stream to serve as an end-of-LOs marker, allowing the restore process to recognize when all large objects have been processed.

## Parameters / Member Variables
- `AH`: Archive handle containing the archive state and format-specific data
- `te`: Table of contents entry for the BLOB data section being finalized

## Dependencies
- Functions called/Symbols referenced:
  - [WriteInt](../W/WriteInt.md)
  - [TocEntry](../T/TocEntry.md) (type)
- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (registered as callback)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md) (referenced in directory format)
  - [InitArchiveFmt_Null](../I/InitArchiveFmt_Null.md) (referenced in null format)
  - [_PrintTocData](../P/_PrintTocData.md) (in null format)

## Notes and Other Information
This function is marked as optional in the archiver interface and is specific to the custom format implementation. The use of a zero OID as an end marker is a simple but effective way to delimit the large objects section, as zero is not a valid OID for actual database objects.