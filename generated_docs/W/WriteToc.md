# WriteToc

## Location
src/bin/pg_dump/pg_backup_archiver.c: 2589 - 2648

## Overview
Writes the Table of Contents (TOC) to the archive, serializing metadata for all database objects that will be included in the dump.

## Definition
```c
void WriteToc(ArchiveHandle *AH)
```

## Detailed Description
This function serializes the complete Table of Contents structure to the archive file. It first counts all entries that have valid requirements (schema, data, or special objects), then writes each entry's metadata in a structured format. The function handles the persistence of critical information needed for later restoration, including object identifiers, dependencies, SQL statements, and various object properties. The TOC serves as the master index that allows pg_restore to understand the structure and relationships of all objects in the dump.

## Parameters / Member Variables
- `AH`: Archive handle containing the TOC linked list and write functions for the specific archive format

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md) (struct type)
  - REQ_SCHEMA, REQ_DATA, REQ_SPECIAL (requirement flags)
  - [WriteInt](WriteInt.md) (writes integer values to archive)
  - [WriteStr](WriteStr.md) (writes string values to archive)
  - WriteExtraTocPtr (optional format-specific extension point)
- Called from (representative examples):
  - [_CloseArchive](../C/_CloseArchive.md) (in custom, directory, and tar format implementations)

## Notes and Other Information
- Only writes entries that have at least one of REQ_SCHEMA, REQ_DATA, or REQ_SPECIAL requirements set
- Object OIDs are written as strings for historical compatibility reasons
- Dependencies are written as a list of dump IDs terminated by a NULL string
- The dataDumper field is written as 1 if present, 0 if NULL, indicating whether the object has associated data
- Includes an extension point (WriteExtraTocPtr) for format-specific additional metadata
- The function writes a hardcoded "false" value that appears to be a legacy field
- Each TOC entry includes comprehensive metadata: dump ID, OIDs, tag, description, section, SQL statements, namespace, tablespace, table access method, relation kind, and owner
- This function is critical for archive integrity as it creates the roadmap for restoration operations