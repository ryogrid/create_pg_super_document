# pgoutput_column_list_init

## Location
[src/backend/replication/pgoutput/pgoutput.c:1041-1155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1041-L1155)

## Overview
Initializes column list filtering for a relation in the pgoutput logical replication plugin by building a bitmap of published columns from multiple publications.

## Definition

```c
static void
pgoutput_column_list_init(PGOutputData *data, List *publications,
						  RelationSyncEntry *entry)
```
## Detailed Description
This function processes column list definitions from multiple publications for a specific relation and creates a unified column bitmap for logical replication filtering. It examines each publication to find column list specifications and ensures consistency across publications - if different publications specify different column lists for the same table, an error is raised. The function handles special cases where "FOR ALL TABLES" or schema-based publications disable column filtering. When a column list includes all live (non-dropped, non-generated) columns, it optimizes by setting the column list to NULL, effectively disabling column filtering for that relation.

## Parameters / Member Variables
- `*data`: Pointer to PGOutputData structure containing plugin global state including memory contexts
- `*publications`: List of Publication structures that may contain column list definitions for this relation
- `*entry`: Pointer to RelationSyncEntry where the computed column bitmap will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [RelationIdGetRelation](../R/RelationIdGetRelation.md)
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [pgoutput_ensure_entry_cxt](pgoutput_ensure_entry_cxt.md)
  - [pub_collist_to_bitmapset](pub_collist_to_bitmapset.md)
  - RelationGetDescr
  - TupleDescAttr
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_free](../b/bms_free.md)
  - [bms_equal](../b/bms_equal.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - RelationGetNamespace
  - [RelationClose](../R/RelationClose.md)
- Called from (representative examples):
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)

## Notes and Other Information
- The function enforces that all publications must have identical column lists for the same table, raising an error if they differ
- Publications with "alltables" flag or schema-based publications implicitly disable column filtering
- Optimization: when column list includes all live columns, it's set to NULL to disable filtering overhead
- Column lists exclude dropped and generated columns from consideration
- Uses the entry's private memory context for bitmap allocations via pgoutput_ensure_entry_cxt
- Static function only accessible within pgoutput.c  
- Part of the lazy initialization pattern for relation synchronization entries
- Critical for implementing selective column replication in logical replication setups

## Simplified Source

```c
static void
pgoutput_column_list_init(PGOutputData *data, List *publications,
                          RelationSyncEntry *entry)
{
    ListCell *lc;
    bool first = true;
    Relation relation = RelationIdGetRelation(entry->publish_as_relid);

    // Process each publication to find column lists
    foreach(lc, publications)
    {
        Publication *pub = lfirst(lc);
        HeapTuple cftuple = NULL;
        Datum cfdatum = 0;
        Bitmapset *cols = NULL;

        // Skip "FOR ALL TABLES" publications
        if (!pub->alltables)
        {
            bool pub_no_list = true;

            // Look for column list in this publication
            cftuple = SearchSysCache2(PUBLICATIONRELMAP,
                                     ObjectIdGetDatum(entry->publish_as_relid),
                                     ObjectIdGetDatum(pub->oid));

            if (HeapTupleIsValid(cftuple))
            {
                // Get column list attribute
                cfdatum = SysCacheGetAttr(PUBLICATIONRELMAP, cftuple,
                                         Anum_pg_publication_rel_prattrs,
                                         &pub_no_list);

                // Build column bitmap if list exists
                if (!pub_no_list)
                {
                    int nliveatts = 0;
                    TupleDesc desc = RelationGetDescr(relation);

                    pgoutput_ensure_entry_cxt(data, entry);
                    cols = pub_collist_to_bitmapset(cols, cfdatum, entry->entry_cxt);

                    // Count live attributes (non-dropped, non-generated)
                    for (int i = 0; i < desc->natts; i++)
                    {
                        Form_pg_attribute att = TupleDescAttr(desc, i);
                        if (!att->attisdropped && !att->attgenerated)
                            nliveatts++;
                    }

                    // Optimize: if all columns included, set to NULL
                    if (bms_num_members(cols) == nliveatts)
                    {
                        bms_free(cols);
                        cols = NULL;
                    }
                }

                ReleaseSysCache(cftuple);
            }
        }

        // Handle first publication or check consistency
        if (first)
        {
            entry->columns = cols;
            first = false;
        }
        else if (!bms_equal(entry->columns, cols))
        {
            // Error: different column lists across publications
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cannot use different column lists for table \"%s.%s\" in different publications",
                            get_namespace_name(RelationGetNamespace(relation)),
                            RelationGetRelationName(relation))));
        }
    }

    RelationClose(relation);
}
```