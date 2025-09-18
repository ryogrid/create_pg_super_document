# EndRestoreLO

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1522 - 1547

## Overview
Finalizes the restoration of a single Large Object by flushing any remaining buffered data and closing the LO handle.

## Definition
```c
void EndRestoreLO(ArchiveHandle *AH, Oid oid)
```

## Detailed Description
This function completes the restoration of an individual Large Object. It ensures that any remaining data in the LO buffer is written to the database by calling dump_lo_buf, then closes the Large Object handle. The function resets the writingLO flag to indicate that LO restoration is complete and cleans up the file descriptor. It works in both connected mode (using libpq LO functions) and disconnected mode (generating SQL statements).

## Parameters / Member Variables
- `AH`: Archive handle containing restoration context and LO file descriptor
- `oid`: Object ID of the Large Object being finalized (for reference)

## Dependencies
- Functions called/Symbols referenced:
  - [dump_lo_buf](../d/dump_lo_buf.md)
  - [lo_close](../l/lo_close.md)
  - [ahprintf](../a/ahprintf.md)
- Called from (representative examples):
  - [_LoadLOs](../L/_LoadLOs.md) (in pg_backup_custom.c, pg_backup_directory.c, pg_backup_tar.c)

## Notes and Other Information
- Flushes any remaining data in the LO buffer before closing to ensure complete data transfer
- Sets writingLO flag to false to indicate LO restoration is no longer active
- In connected mode, uses lo_close() and resets loFd to -1
- In disconnected mode, generates "SELECT pg_catalog.lo_close(0);" SQL statement
- Must be called after StartRestoreLO to properly complete the LO restoration process