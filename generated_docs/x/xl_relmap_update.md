# xl_relmap_update

## Location
src/include/utils/relmapper.h: 27 - 33

## Overview
A WAL (Write-Ahead Log) record structure that represents updates to PostgreSQL's relation mapping files, which maintain the mapping between relation OIDs and their physical file names.

## Definition

```c
typedef struct xl_relmap_update
{
	Oid			dbid;			/* database ID, or 0 for shared map */
	Oid			tsid;			/* database's tablespace, or pg_global */
	int32		nbytes;			/* size of relmap data */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} xl_relmap_update;
```
## Detailed Description
The  structure is a WAL record type used to log changes to PostgreSQL's relation mapping files. These mapping files are critical system files that maintain the correspondence between logical relation OIDs and their physical file numbers on disk. When PostgreSQL needs to update these mappings (such as during table rewrites, index rebuilds, or system catalog changes), it creates a WAL record of this type to ensure crash recovery can properly reconstruct the mapping state.

The structure contains the essential information needed to identify which database and tablespace the mapping update applies to, along with the size and content of the new mapping data. During WAL replay, this information is used to reconstruct the relation mapping files and maintain system consistency.

## Parameters / Member Variables
- : Database OID that the mapping update applies to, or 0 if updating the shared relation map (for system catalogs shared across all databases)
- : Tablespace OID where the database resides, typically either a user-defined tablespace or pg_global for shared system catalogs
- : Size in bytes of the relation mapping data stored in the data field, used for validation during WAL replay
- : Variable-length field containing the actual RelMapFile structure data that represents the new mapping state

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (for variable-length data field)
- Called from (representative examples):
  - write_relmap_file (creates WAL records using this structure)
  - relmap_redo (processes WAL records of this type during recovery)
  - relmap_desc (describes WAL records for debugging/logging)
  - MinSizeOfRelmapUpdate (macro that calculates minimum size)

## Notes and Other Information
- This structure is used specifically for WAL logging and crash recovery of relation mapping changes
- The  field contains a complete RelMapFile structure, which includes magic numbers, CRC checksums, and the actual OID-to-file mappings
- During WAL replay, the  field is validated to ensure it matches  to prevent corruption
- The structure supports both shared relation maps (dbid=0) used for system catalogs and database-specific maps
- The MinSizeOfRelmapUpdate macro calculates the fixed portion size (excluding the variable data field)
- Critical for maintaining data consistency across crashes and ensuring proper file access after recovery