# create_rel_filename_map

## Location
[src/bin/pg_upgrade/info.c:162-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L162-L210)

## Overview
Creates a file mapping structure that associates relation files between old and new PostgreSQL clusters, handling tablespace differences and preserving database and relation file identifiers.

## Definition

```c
enumbers are preserved between old and new cluster */
	map->db_oid = old_db->db_oid;
```
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
  - [DbInfo](../D/DbInfo.md)
  - [RelInfo](../R/RelInfo.md)
  - FileNameMap
- Called from (representative examples):
  - [gen_db_file_maps](../g/gen_db_file_maps.md)

## Notes and Other Information
- Static function - only accessible within the same source file (info.c)
- Handles both default tablespace (empty tablespace string) and custom tablespace cases
- Database OID and relation file number are preserved between clusters during upgrade
- The function assumes old and new relations have identical namespace and relation names for logging purposes
- Part of pg_upgrade's file mapping infrastructure for PostgreSQL major version upgrades
- Tablespace suffix handling depends on global cluster configuration variables

## Simplified Source

```c
static void
create_rel_filename_map(const char *old_data, const char *new_data,
                        const DbInfo *old_db, const DbInfo *new_db,
                        const RelInfo *old_rel, const RelInfo *new_rel,
                        FileNameMap *map)
{
    // Configure old cluster tablespace paths
    if (strlen(old_rel->tablespace) == 0)
    {
        // Relation in default tablespace - use data directory
        map->old_tablespace = old_data;
        map->old_tablespace_suffix = "/base";
    }
    else
    {
        // Relation in custom tablespace - use tablespace location
        map->old_tablespace = old_rel->tablespace;
        map->old_tablespace_suffix = old_cluster.tablespace_suffix;
    }

    // Configure new cluster tablespace paths
    if (strlen(new_rel->tablespace) == 0)
    {
        // Relation in default tablespace - use data directory
        map->new_tablespace = new_data;
        map->new_tablespace_suffix = "/base";
    }
    else
    {
        // Relation in custom tablespace - use tablespace location
        map->new_tablespace = new_rel->tablespace;
        map->new_tablespace_suffix = new_cluster.tablespace_suffix;
    }

    // Preserve database and relation identifiers (must remain unchanged)
    map->db_oid = old_db->db_oid;
    map->relfilenumber = old_rel->relfilenumber;

    // Set relation names for logging and error reporting
    map->nspname = old_rel->nspname;
    map->relname = old_rel->relname;
}
```