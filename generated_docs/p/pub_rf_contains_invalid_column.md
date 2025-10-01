# pub_rf_contains_invalid_column

## Location
[src/backend/commands/publicationcmds.c:258-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L258-L333)

## Overview
Validates whether a publication's row filter expression references only columns that are part of the table's REPLICA IDENTITY.

## Definition

```c
bool
pub_rf_contains_invalid_column(Oid pubid, Relation relation, List *ancestors,
							   bool pubviaroot)
```
## Detailed Description
This function performs validation to ensure that all columns referenced in a publication's row filter WHERE clause are part of the table's REPLICA IDENTITY. This validation is critical for logical replication because only columns in the REPLICA IDENTITY are guaranteed to be available on the subscriber side for filtering operations.

The function first checks for REPLICA IDENTITY FULL, which includes all columns and thus allows any column reference in the row filter. For partitioned tables with pubviaroot enabled, it finds the topmost published ancestor and uses that table's row filter expression while validating against the actual partition's REPLICA IDENTITY.

The validation process involves retrieving the row filter expression from the pg_publication_rel catalog, converting it from text to a Node tree, and then using a tree walker to examine all column references in the expression.

## Parameters / Member Variables
- : OID of the publication to validate
- : The relation being validated for row filter compatibility
- : List of ancestor relations (used for partitioned tables)
- : Boolean indicating whether to publish via partition root

## Dependencies
- Functions called/Symbols referenced:
  - [GetTopMostAncestorInPublication](../G/GetTopMostAncestorInPublication.md)
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [RelationGetIndexAttrBitmap](../R/RelationGetIndexAttrBitmap.md)
  - [stringToNode](../s/stringToNode.md)
  - TextDatumGetCString
  - [contain_invalid_rfcolumn_walker](../c/contain_invalid_rfcolumn_walker.md)
  - REPLICA_IDENTITY_FULL
  - INDEX_ATTR_BITMAP_IDENTITY_KEY
  - [rf_context](../r/rf_context.md)
- Called from (representative examples):
  - [RelationBuildPublicationDesc](../R/RelationBuildPublicationDesc.md)
  - MAX_RELCACHE_INVAL_MSGS (referenced in header)

## Notes and Other Information
- Returns true if any referenced column is NOT in the replica identity, false if all columns are valid
- Short-circuits validation for REPLICA IDENTITY FULL since all columns are allowed
- Handles complex partitioning scenarios where row filters are inherited from ancestors
- Uses system cache lookups to retrieve row filter expressions from pg_publication_rel
- Creates a bitmap of REPLICA IDENTITY columns for efficient validation
- Returns false if no row filter is defined (rfisnull case)
- Located in src/backend/commands/publicationcmds.c:258-333

## Simplified Source

```c
bool pub_rf_contains_invalid_column(Oid pubid, Relation relation,
                                   List *ancestors, bool pubviaroot) {
    Oid relid = RelationGetRelid(relation);
    Oid publish_as_relid = relid;

    // REPLICA IDENTITY FULL allows all columns in row filters
    if (relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL)
        return false;

    // For partitions with pubviaroot, find the topmost ancestor
    if (pubviaroot && relation->rd_rel->relispartition) {
        publish_as_relid = GetTopMostAncestorInPublication(pubid, ancestors, NULL);
        if (!OidIsValid(publish_as_relid))
            publish_as_relid = relid;
    }

    // Look up the row filter for this publication-relation pair
    HeapTuple rftuple = SearchSysCache2(PUBLICATIONRELMAP,
                                       ObjectIdGetDatum(publish_as_relid),
                                       ObjectIdGetDatum(pubid));
    if (!HeapTupleIsValid(rftuple))
        return false;

    Datum rfdatum;
    bool rfisnull;
    rfdatum = SysCacheGetAttr(PUBLICATIONRELMAP, rftuple,
                             Anum_pg_publication_rel_prqual, &rfisnull);

    bool result = false;
    if (!rfisnull) {
        // Set up context for column validation
        rf_context context = {0};
        context.pubviaroot = pubviaroot;
        context.parentid = publish_as_relid;
        context.relid = relid;

        // Get replica identity columns for validation
        context.bms_replident = RelationGetIndexAttrBitmap(relation,
                                                          INDEX_ATTR_BITMAP_IDENTITY_KEY);

        // Parse and validate the row filter expression
        Node *rfnode = stringToNode(TextDatumGetCString(rfdatum));
        result = contain_invalid_rfcolumn_walker(rfnode, &context);
    }

    ReleaseSysCache(rftuple);
    return result;
}
```