# gen_db_file_maps

## Location
[src/bin/pg_upgrade/info.c:42-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L42-L161)

## Overview
Generates a database mapping from an old database to a new database during PostgreSQL upgrade operations, creating file mappings for relation files between the old and new clusters.

## Definition


## Detailed Description
This function is a core component of pg_upgrade that creates mappings between relation files in the old and new PostgreSQL clusters. It compares the RelInfo arrays of both databases (which should be sorted by OID) and matches relations between the old and new versions. The function performs validation to ensure that relations with the same OID have matching names and handles cases where relations don't match properly.

The function implements a two-pointer algorithm to traverse through the sorted relation arrays, creating file mappings for matched relations and reporting errors for unmatched ones. It's particularly careful about handling TOAST tables, which may be created automatically by the new server and might not have exact matches in the old cluster.

## Parameters / Member Variables
- : Pointer to DbInfo structure containing information about the database in the old cluster
- : Pointer to DbInfo structure containing information about the database in the new cluster  
- : Output parameter that receives the number of mappings created
- : Path to the old PostgreSQL data directory
- : Path to the new PostgreSQL data directory

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc
  - [report_unmatched_relation](../r/report_unmatched_relation.md)
  - [create_rel_filename_map](../c/create_rel_filename_map.md)
  - [pg_log](../p/pg_log.md)
  - [pg_fatal](../p/pg_fatal.md)
- Data structures used:
  - [DbInfo](../D/DbInfo.md)
  - FileNameMap
  - [RelInfo](../R/RelInfo.md)
- Called from (representative examples):
  - [transfer_all_new_dbs](../t/transfer_all_new_dbs.md)

## Notes and Other Information
- Returns a malloc'ed array of FileNameMap structures that must be freed by the caller
- The function will abort the upgrade process if it fails to match all relations between old and new databases
- Special handling for pg_toast namespace relations, which may exist in the new cluster without corresponding relations in the old cluster
- The relation arrays are assumed to be pre-sorted by OID for efficient matching
- Part of the pg_upgrade utility's file transfer mechanism for PostgreSQL major version upgrades