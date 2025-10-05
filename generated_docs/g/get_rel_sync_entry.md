# get_rel_sync_entry

## Location
[src/backend/replication/pgoutput/pgoutput.c:2002-2293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L2002-L2293)

## Overview
Finds or creates an entry in the relation schema cache for PostgreSQL logical replication, determining which publications a relation participates in and what operations should be replicated.

## Definition

```c
static RelationSyncEntry *
get_rel_sync_entry(PGOutputData *data, Relation relation)
```
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
- `*data`: Pointer to PGOutputData containing publication names and configuration
- `relation`: The PostgreSQL relation for which to find or create a sync entry
## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md) (cache lookup)
  - RelationGetRelid (get relation OID)
  - [get_rel_namespace](get_rel_namespace.md) (get schema OID)
  - [GetRelationPublications](../G/GetRelationPublications.md) (get relation's publications)
  - [GetSchemaPublications](../G/GetSchemaPublications.md) (get schema's publications) 
  - [get_rel_relispartition](get_rel_relispartition.md) (check if partition)
  - [get_partition_ancestors](get_partition_ancestors.md) (get partition hierarchy)
  - [GetTopMostAncestorInPublication](../G/GetTopMostAncestorInPublication.md) (find published ancestor)
  - [init_tuple_slot](../i/init_tuple_slot.md) (initialize tuple storage)
  - [pgoutput_row_filter_init](../p/pgoutput_row_filter_init.md) (setup row filtering)
  - [pgoutput_column_list_init](../p/pgoutput_column_list_init.md) (setup column lists)
  - [LoadPublications](../L/LoadPublications.md) (reload publication data)
- Called from (representative examples):
  - [pgoutput_change](../p/pgoutput_change.md) (during change processing)
  - [pgoutput_truncate](../p/pgoutput_truncate.md) (during truncate processing)

## Notes and Other Information
- The function uses a global RelationSyncCache hash table to store entries
- Entries are invalidated and rebuilt when publication configurations change
- Memory management includes cleanup of tuple slots, attribute maps, and expression states
- Special handling for partitioned tables based on pubviaroot publication settings
- The function supports both direct relation publication and schema-level publication
- Row filters and column lists are only initialized when DML operations are published

## Simplified Source

```c
static RelationSyncEntry *
get_rel_sync_entry(PGOutputData *data, Relation relation) {
    Oid relid = RelationGetRelid(relation);
    bool found;

    // Find or create cache entry
    RelationSyncEntry *entry = hash_search(RelationSyncCache, &relid, HASH_ENTER, &found);

    // Initialize new entry
    if (!found) {
        entry->replicate_valid = false;
        entry->schema_sent = false;
        entry->streamed_txns = NIL;
        entry->pubactions.pubinsert = entry->pubactions.pubupdate =
            entry->pubactions.pubdelete = entry->pubactions.pubtruncate = false;
        entry->new_slot = entry->old_slot = NULL;
        entry->publish_as_relid = InvalidOid;
        entry->columns = NULL;
        entry->attrmap = NULL;
        // ... other initialization
    }

    // Validate and rebuild entry if needed
    if (!entry->replicate_valid) {
        // Get publication lists for this relation and its schema
        List *pubids = GetRelationPublications(relid);
        List *schemaPubids = GetSchemaPublications(get_rel_namespace(relid));

        // Reload publications if needed
        if (!publications_valid) {
            data->publications = LoadPublications(data->publication_names);
            publications_valid = true;
        }

        // Reset entry state
        entry->schema_sent = false;
        list_free(entry->streamed_txns);
        entry->streamed_txns = NIL;
        // ... cleanup old slots, maps, filters

        // Determine publication actions and ancestor relationships
        Oid publish_as_relid = relid;
        int publish_ancestor_level = 0;
        bool am_partition = get_rel_relispartition(relid);
        List *rel_publications = NIL;

        // Process each publication to determine actions
        foreach(lc, data->publications) {
            Publication *pub = lfirst(lc);
            bool publish = false;
            Oid pub_relid = relid;
            int ancestor_level = 0;

            // Check if this publication applies to our relation
            if (pub->alltables) {
                publish = true;
                if (pub->pubviaroot && am_partition) {
                    List *ancestors = get_partition_ancestors(relid);
                    pub_relid = llast_oid(ancestors);
                    ancestor_level = list_length(ancestors);
                }
            } else {
                // Check direct publication, schema publication, or ancestor publication
                if (list_member_oid(pubids, pub->oid) ||
                    list_member_oid(schemaPubids, pub->oid)) {
                    publish = true;
                } else if (am_partition) {
                    // Check if any ancestor is published
                    List *ancestors = get_partition_ancestors(relid);
                    Oid ancestor = GetTopMostAncestorInPublication(pub->oid, ancestors, &ancestor_level);
                    if (ancestor != InvalidOid) {
                        publish = true;
                        if (pub->pubviaroot) {
                            pub_relid = ancestor;
                        }
                    }
                }
            }

            // Set publication actions if this publication applies
            if (publish && (get_rel_relkind(relid) != RELKIND_PARTITIONED_TABLE || pub->pubviaroot)) {
                entry->pubactions.pubinsert |= pub->pubactions.pubinsert;
                entry->pubactions.pubupdate |= pub->pubactions.pubupdate;
                entry->pubactions.pubdelete |= pub->pubactions.pubdelete;
                entry->pubactions.pubtruncate |= pub->pubactions.pubtruncate;

                // Track the topmost ancestor across all publications
                if (publish_ancestor_level <= ancestor_level) {
                    if (publish_ancestor_level < ancestor_level) {
                        publish_as_relid = pub_relid;
                        publish_ancestor_level = ancestor_level;
                        rel_publications = NIL;
                    }
                    rel_publications = lappend(rel_publications, pub);
                }
            }
        }

        entry->publish_as_relid = publish_as_relid;

        // Initialize tuple slots, row filters, and column lists for DML operations
        if (entry->pubactions.pubinsert || entry->pubactions.pubupdate || entry->pubactions.pubdelete) {
            init_tuple_slot(data, relation, entry);
            pgoutput_row_filter_init(data, rel_publications, entry);
            pgoutput_column_list_init(data, rel_publications, entry);
        }

        // Cleanup and mark as valid
        list_free(pubids);
        list_free(schemaPubids);
        list_free(rel_publications);
        entry->replicate_valid = true;
    }

    return entry;
}
```