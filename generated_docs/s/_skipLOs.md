# _skipLOs

## Location
src/bin/pg_dump/pg_backup_custom.c: 605 - 622

## Overview
Skips Large Object (LO) data blocks from the current file position in a PostgreSQL custom format archive during restore operations.

## Definition
```c
static void _skipLOs(ArchiveHandle *AH)
```

## Detailed Description
This function is responsible for skipping over Large Object data in a PostgreSQL custom format dump file. Large Objects are stored sequentially as data blocks, with each LO preceded by its original OID. The function reads OIDs sequentially and skips the associated data blocks until it encounters a zero OID, which indicates the end of the Large Objects section.

The function implements a simple loop that:
1. Reads an OID from the archive
2. If the OID is non-zero, skips the associated data block and continues
3. If the OID is zero, terminates the loop as this marks the end of LOs

## Parameters / Member Variables
- `AH`: Archive handle containing the state and context for the dump/restore operation

## Dependencies
- Functions called/Symbols referenced:
  - [ReadInt](../R/ReadInt.md): Reads integer values (OIDs) from the archive
  - [_skipData](_skipData.md): Skips data blocks associated with each Large Object
- Called from (representative examples):
  - [_PrintTocData](../P/_PrintTocData.md): Used during table of contents processing in custom format archives

## Notes and Other Information
- This is a static function specific to the custom format archive handling
- Large Objects in PostgreSQL dumps are stored with a specific format where each LO is preceded by its OID
- A zero OID serves as a sentinel value to mark the end of the LO sequence
- The function is part of the pg_dump/pg_restore custom format implementation
- File location: src/bin/pg_dump/pg_backup_custom.c:605-622