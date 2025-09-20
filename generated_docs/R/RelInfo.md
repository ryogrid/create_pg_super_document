# RelInfo

## Location
[src/bin/pg_upgrade/pg_upgrade.h:145-150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.h#L145-L150)

## Overview
RelInfo is a structure that stores metadata information for a PostgreSQL relation (table, index, or toast table) used during the pg_upgrade process to map relations between old and new clusters.

## Definition

```c
typedef struct
{
	RelInfo    *rels;
	int			nrels;
} RelInfoArr;
```
## Detailed Description
RelInfo is a fundamental data structure in pg_upgrade that encapsulates all essential metadata for a database relation during the upgrade process. It serves as a bridge between the old and new PostgreSQL clusters, ensuring that relation mappings are correctly maintained. The structure is designed to be portable across different PostgreSQL versions, which is why it avoids using NAMEDATALEN directly.

## Parameters / Member Variables
- : Namespace (schema) name containing the relation
- : The actual name of the relation (table, index, etc.)
- : Object identifier of the relation in the PostgreSQL system catalogs
- : Physical file number used to store the relation data
- : For indexes, contains the OID of the parent table; 0 for non-indexes
- : For TOAST tables, contains the OID of the base table; 0 for non-TOAST tables
- : Path to the tablespace where the relation is stored; empty string for cluster default
- : Boolean flag indicating whether the nspname memory should be freed
- : Boolean flag indicating whether the tablespace memory should be freed

## Dependencies
- Functions called/Symbols referenced:
  - Oid
  - [RelFileNumber](RelFileNumber.md)
- Called from (representative examples):
  - [gen_db_file_maps](../g/gen_db_file_maps.md)
  - [create_rel_filename_map](../c/create_rel_filename_map.md)
  - [report_unmatched_relation](../r/report_unmatched_relation.md)
  - [get_rel_infos](../g/get_rel_infos.md)

## Notes and Other Information
- This structure is specifically designed for pg_upgrade utility and should not be confused with similar structures used in the main PostgreSQL server
- The memory management flags (nsp_alloc, tblsp_alloc) are important for proper cleanup during the upgrade process
- The structure handles special relation types like indexes and TOAST tables through the indtable and toastheap fields
- [RelFileNumber](RelFileNumber.md) type is used instead of plain integers for better type safety in file operations