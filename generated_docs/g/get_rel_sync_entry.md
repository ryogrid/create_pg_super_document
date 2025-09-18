# get_rel_sync_entry

## Location
src/backend/replication/pgoutput/pgoutput.c: 2002 - 2293

## Overview
Finds or creates an entry in the relation schema cache for PostgreSQL logical replication, determining which publications a relation participates in and what operations should be replicated.

## Definition


## Detailed Description
This function manages a hash-based cache of relation synchronization entries for logical replication output. It performs the following key operations:

1. **Cache Lookup**: Searches the RelationSyncCache hash table for an existing entry using the relation's OID
2. **Entry Initialization**: If no entry exists, creates and initializes a new RelationSyncEntry with default values
3. **Publication Analysis**: Determines which publications the relation participates in, either directly or through schema membership
4. **Ancestor Resolution**: For partitioned tables, identifies the appropriate ancestor relation to use for publication based on pubviaroot settings
5. **Action Determination**: Sets publication actions (insert, update, delete, truncate) based on publication configurations
6. **Resource Management**: Manages tuple slots, attribute maps, row filters, and column lists for the entry
7. **Cache Validation**: Marks the entry as valid after processing all publication information

The function handles complex scenarios including partitioned tables, schema-level publications, and ancestor-based publishing rules.

## Parameters / Member Variables
- : Pointer to PGOutputData containing publication names and configuration
- : The PostgreSQL relation for which to find or create a sync entry

## Dependencies
- Functions called/Symbols referenced:
  - hash_search (cache lookup)
  - RelationGetRelid (get relation OID)
  - get_rel_namespace (get schema OID)
  - GetRelationPublications (get relation's publications)
  - GetSchemaPublications (get schema's publications) 
  - get_rel_relispartition (check if partition)
  - get_partition_ancestors (get partition hierarchy)
  - GetTopMostAncestorInPublication (find published ancestor)
  - init_tuple_slot (initialize tuple storage)
  - pgoutput_row_filter_init (setup row filtering)
  - pgoutput_column_list_init (setup column lists)
  - LoadPublications (reload publication data)
- Called from (representative examples):
  - pgoutput_change (during change processing)
  - pgoutput_truncate (during truncate processing)

## Notes and Other Information
- The function uses a global RelationSyncCache hash table to store entries
- Entries are invalidated and rebuilt when publication configurations change
- Memory management includes cleanup of tuple slots, attribute maps, and expression states
- Special handling for partitioned tables based on pubviaroot publication settings
- The function supports both direct relation publication and schema-level publication
- Row filters and column lists are only initialized when DML operations are published