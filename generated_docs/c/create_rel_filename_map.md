# create_rel_filename_map

## Location
src/bin/pg_upgrade/info.c: 162 - 210

## Overview
Creates a file mapping structure that associates relation files between old and new PostgreSQL clusters, handling tablespace differences and preserving database and relation file identifiers.

## Definition


## Detailed Description
This static helper function populates a FileNameMap structure with the necessary information to map a relation file from the old cluster to its corresponding location in the new cluster. The function handles both default tablespace relations (stored in the base directory) and custom tablespace relations, setting appropriate paths and suffixes for each case.

The function preserves critical identifiers like database OID and relation file number between clusters, which is essential for maintaining data consistency during upgrades. It also handles the complexity of tablespace mappings, accounting for different tablespace locations and directory structures between old and new clusters.

## Parameters / Member Variables
- : Path to the old cluster's data directory
- : Path to the new cluster's data directory
- : Database information from the old cluster
- : Database information from the new cluster (currently unused in implementation)
- : Relation information from the old cluster
- : Relation information from the new cluster
- : Output parameter - FileNameMap structure to be populated

## Dependencies
- Functions called/Symbols referenced:
  - (Uses global variables old_cluster.tablespace_suffix and new_cluster.tablespace_suffix)
- Data structures used:
  - DbInfo
  - RelInfo
  - FileNameMap
- Called from (representative examples):
  - gen_db_file_maps

## Notes and Other Information
- Static function - only accessible within the same source file (info.c)
- Handles both default tablespace (empty tablespace string) and custom tablespace cases
- Database OID and relation file number are preserved between clusters during upgrade
- The function assumes old and new relations have identical namespace and relation names for logging purposes
- Part of pg_upgrade's file mapping infrastructure for PostgreSQL major version upgrades
- Tablespace suffix handling depends on global cluster configuration variables