# pgoutput_row_filter_init

## Location
[src/backend/replication/pgoutput/pgoutput.c:895-1040](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L895-L1040)

## Overview
Initializes row filtering functionality for a specific relation in the pgoutput logical replication plugin by analyzing publications and building expression states for DML operations.

## Definition

```c
static void
pgoutput_row_filter_init(PGOutputData *data, List *publications,
						 RelationSyncEntry *entry)
```
## Detailed Description
This function is responsible for initializing row filters for a synchronized relation in logical replication. It examines all relevant publications to determine which row filters apply to the relation and builds the necessary expression states for filtering INSERT, UPDATE, and DELETE operations. The function handles complex logic including "FOR ALL TABLES" and "FOR TABLES IN SCHEMA" publications that take precedence over specific row filters. For each DML operation type, it combines multiple row filter expressions using OR logic and caches the prepared expression states in the RelationSyncEntry for efficient evaluation during replication.

## Parameters / Member Variables
- `*data`: Pointer to PGOutputData structure containing plugin global state including memory contexts
- `*publications`: List of Publication structures that may contain row filter definitions for this relation
- `*entry`: Pointer to RelationSyncEntry where the compiled expression states will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [get_rel_namespace](../g/get_rel_namespace.md)
  - SearchSysCacheExists2
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - TextDatumGetCString
  - [list_free_deep](../l/list_free_deep.md)
  - [RelationIdGetRelation](../R/RelationIdGetRelation.md)
  - [pgoutput_ensure_entry_cxt](pgoutput_ensure_entry_cxt.md)
  - [create_estate_for_relation](../c/create_estate_for_relation.md)
  - [stringToNode](../s/stringToNode.md)
  - [make_orclause](../m/make_orclause.md)
  - [ExecPrepareExpr](../E/ExecPrepareExpr.md)
  - [RelationClose](../R/RelationClose.md)
  - PUBACTION_INSERT/UPDATE/DELETE constants
  - NUM_ROWFILTER_PUBACTIONS
- Called from (representative examples):
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)

## Notes and Other Information
- The function uses a three-element array (rfnodes) to collect row filter expressions separately for INSERT, UPDATE, and DELETE operations
- Publications with "alltables" or matching schema publications disable row filtering entirely for affected operations
- Row filter expressions from multiple publications are combined using OR logic (make_orclause)
- Expression states are prepared and cached in the entry's private memory context for performance
- The function performs early exit optimization when no filtering is needed for any DML operation
- Static function only accessible within pgoutput.c
- Part of the lazy initialization pattern - row filters are only prepared when actually needed

## Simplified Source

```c
static void
pgoutput_row_filter_init(PGOutputData *data, List *publications,
                         RelationSyncEntry *entry)
{
    ListCell *lc;
    List *rfnodes[3] = {NIL, NIL, NIL};  // One per DML action
    bool no_filter[3] = {false, false, false};
    bool has_filter = true;
    Oid schemaid = get_rel_namespace(entry->publish_as_relid);

    // Check each publication for row filters
    foreach(lc, publications)
    {
        Publication *pub = lfirst(lc);
        HeapTuple rftuple = NULL;
        Datum rfdatum = 0;
        bool pub_no_filter = true;

        // Skip if publication covers all tables or schema
        if (!pub->alltables &&
            !SearchSysCacheExists2(PUBLICATIONNAMESPACEMAP,
                                   ObjectIdGetDatum(schemaid),
                                   ObjectIdGetDatum(pub->oid)))
        {
            // Look for row filter in this publication
            rftuple = SearchSysCache2(PUBLICATIONRELMAP,
                                     ObjectIdGetDatum(entry->publish_as_relid),
                                     ObjectIdGetDatum(pub->oid));

            if (HeapTupleIsValid(rftuple))
            {
                rfdatum = SysCacheGetAttr(PUBLICATIONRELMAP, rftuple,
                                         Anum_pg_publication_rel_prqual,
                                         &pub_no_filter);
            }
        }

        if (pub_no_filter)
        {
            // Mark actions as having no filter
            no_filter[PUBACTION_INSERT] |= pub->pubactions.pubinsert;
            no_filter[PUBACTION_UPDATE] |= pub->pubactions.pubupdate;
            no_filter[PUBACTION_DELETE] |= pub->pubactions.pubdelete;

            // Early exit if all actions have no filter
            if (no_filter[PUBACTION_INSERT] &&
                no_filter[PUBACTION_UPDATE] &&
                no_filter[PUBACTION_DELETE])
            {
                has_filter = false;
                break;
            }
        }
        else
        {
            // Add filter expressions to appropriate action lists
            if (pub->pubactions.pubinsert && !no_filter[PUBACTION_INSERT])
                rfnodes[PUBACTION_INSERT] = lappend(rfnodes[PUBACTION_INSERT],
                                                   TextDatumGetCString(rfdatum));
            if (pub->pubactions.pubupdate && !no_filter[PUBACTION_UPDATE])
                rfnodes[PUBACTION_UPDATE] = lappend(rfnodes[PUBACTION_UPDATE],
                                                   TextDatumGetCString(rfdatum));
            if (pub->pubactions.pubdelete && !no_filter[PUBACTION_DELETE])
                rfnodes[PUBACTION_DELETE] = lappend(rfnodes[PUBACTION_DELETE],
                                                   TextDatumGetCString(rfdatum));
        }

        if (rftuple)
            ReleaseSysCache(rftuple);
    }

    // Clean up filter lists for actions that have no_filter
    for (int idx = 0; idx < NUM_ROWFILTER_PUBACTIONS; idx++)
    {
        if (no_filter[idx])
        {
            list_free_deep(rfnodes[idx]);
            rfnodes[idx] = NIL;
        }
    }

    // Build expression states if filters exist
    if (has_filter)
    {
        Relation relation = RelationIdGetRelation(entry->publish_as_relid);
        pgoutput_ensure_entry_cxt(data, entry);

        MemoryContext oldctx = MemoryContextSwitchTo(entry->entry_cxt);
        entry->estate = create_estate_for_relation(relation);

        for (int idx = 0; idx < NUM_ROWFILTER_PUBACTIONS; idx++)
        {
            if (rfnodes[idx] != NIL)
            {
                List *filters = NIL;

                // Convert string filters to expression nodes
                foreach(lc, rfnodes[idx])
                    filters = lappend(filters, stringToNode((char *) lfirst(lc)));

                // Combine filters with OR and prepare expression
                Expr *rfnode = make_orclause(filters);
                entry->exprstate[idx] = ExecPrepareExpr(rfnode, entry->estate);
            }
        }

        MemoryContextSwitchTo(oldctx);
        RelationClose(relation);
    }
}
```