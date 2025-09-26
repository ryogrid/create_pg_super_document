# StartRestoreLO

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1472-1521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1472-L1521)

## Overview
Initiates the restoration of a single Large Object by setting up the LO buffer, opening the LO for writing, and handling version-specific creation logic.

## Definition
```c
void StartRestoreLO(ArchiveHandle *AH, Oid oid, bool drop)
```

## Detailed Description
This function prepares for the restoration of an individual Large Object. It initializes the LO buffer if needed, increments the LO counter, and opens the Large Object for writing. The function handles differences between old and new archive formats - older archives require explicit LO creation and drop logic, while newer formats handle this differently. It works in both connected mode (direct database operations) and disconnected mode (generating SQL statements).

## Parameters / Member Variables
- `AH`: Archive handle containing restoration context and buffers
- `oid`: Object ID of the Large Object to restore
- `drop`: Whether to drop the LO if it exists (used with old archive format)

## Dependencies
- Functions called/Symbols referenced:
  - K_VERS_1_12
  - LOBBUFSIZE
  - [pg_malloc](../p/pg_malloc.md)
  - pg_log_info
  - [DropLOIfExists](../D/DropLOIfExists.md)
  - [lo_create](../l/lo_create.md)
  - [lo_open](../l/lo_open.md)
  - INV_WRITE
  - [ahprintf](../a/ahprintf.md)
- Called from (representative examples):
  - [_LoadLOs](../L/_LoadLOs.md) (in pg_backup_custom.c, pg_backup_directory.c, pg_backup_tar.c)

## Notes and Other Information
- Allocates LO buffer (LOBBUFSIZE) on first use per process
- Handles version differences: old archives (< K_VERS_1_12) require explicit lo_create calls
- Sets writingLO flag to true to indicate LO restoration is in progress
- In connected mode, uses libpq LO functions; in disconnected mode, generates SQL
- Provides user feedback by logging the OID being restored
- The LO is opened with INV_WRITE permission for data writing