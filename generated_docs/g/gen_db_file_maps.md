# gen_db_file_maps

## Location
[src/bin/pg_upgrade/info.c:42-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L42-L161)

## Overview
Generates a database mapping from an old database to a new database during PostgreSQL upgrade operations, creating file mappings for relation files between the old and new clusters.

## Definition

```c
FileNameMap *
gen_db_file_maps(DbInfo *old_db, DbInfo *new_db,
				 int *nmaps,
				 const char *old_pgdata, const char *new_pgdata)
```
## Detailed Description
This function is a core component of pg_upgrade that creates mappings between relation files in the old and new PostgreSQL clusters. It compares the RelInfo arrays of both databases (which should be sorted by OID) and matches relations between the old and new versions. The function performs validation to ensure that relations with the same OID have matching names and handles cases where relations don't match properly.

The function implements a two-pointer algorithm to traverse through the sorted relation arrays, creating file mappings for matched relations and reporting errors for unmatched ones. It's particularly careful about handling TOAST tables, which may be created automatically by the new server and might not have exact matches in the old cluster.

## Parameters / Member Variables
- `*old_db`: Pointer to DbInfo structure containing information about the database in the old cluster
- `*new_db`: Pointer to DbInfo structure containing information about the database in the new cluster
- `*nmaps`: Output parameter that receives the number of mappings created
- `*old_pgdata`: Path to the old PostgreSQL data directory
- `*new_pgdata`: Path to the new PostgreSQL data directory
## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
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

## Simplified Source

```c
FileNameMap *
gen_db_file_maps(DbInfo *old_db, DbInfo *new_db,
                 int *nmaps,
                 const char *old_pgdata, const char *new_pgdata)
{
    FileNameMap *file_maps;
    int old_rel_index = 0, new_rel_index = 0;
    int num_mappings = 0;
    bool all_relations_matched = true;

    // Allocate memory for mappings (max possible = number of old relations)
    file_maps = pg_malloc(sizeof(FileNameMap) * old_db->rel_arr.nrels);

    // Two-pointer algorithm: match relations by OID between old and new clusters
    while (old_rel_index < old_db->rel_arr.nrels ||
           new_rel_index < new_db->rel_arr.nrels)
    {
        RelInfo *old_relation = (old_rel_index < old_db->rel_arr.nrels) ?
            &old_db->rel_arr.rels[old_rel_index] : NULL;
        RelInfo *new_relation = (new_rel_index < new_db->rel_arr.nrels) ?
            &new_db->rel_arr.rels[new_rel_index] : NULL;

        // Handle case where one array is exhausted
        if (!new_relation)
        {
            // Old relation has no match in new cluster (should not happen)
            report_unmatched_relation(old_relation, old_db, false);
            all_relations_matched = false;
            old_rel_index++;
            continue;
        }
        if (!old_relation)
        {
            // New relation has no match in old cluster (acceptable for TOAST tables)
            if (strcmp(new_relation->nspname, "pg_toast") != 0)
            {
                report_unmatched_relation(new_relation, new_db, true);
                all_relations_matched = false;
            }
            new_rel_index++;
            continue;
        }

        // Compare OIDs to determine matching status
        if (old_relation->reloid < new_relation->reloid)
        {
            // Old relation OID is smaller - no match found
            report_unmatched_relation(old_relation, old_db, false);
            all_relations_matched = false;
            old_rel_index++;
        }
        else if (old_relation->reloid > new_relation->reloid)
        {
            // New relation OID is smaller - acceptable if TOAST table
            if (strcmp(new_relation->nspname, "pg_toast") != 0)
            {
                report_unmatched_relation(new_relation, new_db, true);
                all_relations_matched = false;
            }
            new_rel_index++;
        }
        else
        {
            // OIDs match - verify relation names are consistent
            if (strcmp(old_relation->nspname, new_relation->nspname) != 0 ||
                strcmp(old_relation->relname, new_relation->relname) != 0)
            {
                pg_log(PG_WARNING, "Relation names for OID %u in database \"%s\" do not match: "
                       "old name \"%s.%s\", new name \"%s.%s\"",
                       old_relation->reloid, old_db->db_name,
                       old_relation->nspname, old_relation->relname,
                       new_relation->nspname, new_relation->relname);
                all_relations_matched = false;
            }
            else
            {
                // Create file mapping for this matched relation
                create_rel_filename_map(old_pgdata, new_pgdata, old_db, new_db,
                                        old_relation, new_relation, file_maps + num_mappings);
                num_mappings++;
            }
            old_rel_index++;
            new_rel_index++;
        }
    }

    // Abort if any relations failed to match properly
    if (!all_relations_matched)
        pg_fatal("Failed to match up old and new tables in database \"%s\"",
                 old_db->db_name);

    *nmaps = num_mappings;
    return file_maps;
}
```