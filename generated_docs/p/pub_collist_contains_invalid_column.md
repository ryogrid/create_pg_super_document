# pub_collist_contains_invalid_column

## Location
[src/backend/commands/publicationcmds.c:334-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L334-L437)

## Overview
Validates that all columns in a table's REPLICA IDENTITY are covered by the publication's column list, ensuring proper logical replication functionality.

## Definition

```c
bool
pub_collist_contains_invalid_column(Oid pubid, Relation relation, List *ancestors,
									bool pubviaroot)
```
## Detailed Description
This function validates that when a publication specifies a column list for a table, all columns that are part of the table's REPLICA IDENTITY are included in that column list. This validation is crucial for logical replication because REPLICA IDENTITY columns are required on the subscriber side to uniquely identify rows for UPDATE and DELETE operations.

The function handles special logic for partitioned tables when pubviaroot is enabled. In such cases, it uses the column list defined on the topmost published ancestor while validating against the actual partition's REPLICA IDENTITY. Since parent and child tables may have different column ordering, it performs column name translation between the tables.

For REPLICA IDENTITY FULL tables, the function immediately returns true (invalid) because column lists are not allowed when all columns are part of the replica identity.

## Parameters / Member Variables
- : OID of the publication to validate
- : The relation being validated for column list compatibility
- : List of ancestor relations (used for partitioned tables)  
- : Boolean indicating whether to publish via partition root

## Dependencies
- Functions called/Symbols referenced:
  - [GetTopMostAncestorInPublication](../G/GetTopMostAncestorInPublication.md)
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [pub_collist_to_bitmapset](pub_collist_to_bitmapset.md)
  - [RelationGetIndexAttrBitmap](../R/RelationGetIndexAttrBitmap.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [get_attname](../g/get_attname.md)
  - [get_attnum](../g/get_attnum.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_free](../b/bms_free.md)
  - REPLICA_IDENTITY_FULL
  - INDEX_ATTR_BITMAP_IDENTITY_KEY
  - FirstLowInvalidHeapAttributeNumber
- Called from (representative examples):
  - [RelationBuildPublicationDesc](../R/RelationBuildPublicationDesc.md)
  - MAX_RELCACHE_INVAL_MSGS (referenced in header)

## Notes and Other Information
- Returns true if any REPLICA IDENTITY column is missing from the column list, false if all are covered
- Automatically fails validation for REPLICA IDENTITY FULL tables since column lists are incompatible
- Handles attribute number offset differences between REPLICA IDENTITY bitmaps and column list bitmaps
- Performs column name resolution for parent-child table mapping when pubviaroot is enabled
- Retrieves column list from pg_publication_rel catalog's prattrs attribute
- Uses efficient bitmap operations for column membership testing
- Properly manages memory by freeing allocated bitmapsets
- Located in src/backend/commands/publicationcmds.c:334-437

## Simplified Source

```c
bool pub_collist_contains_invalid_column(Oid pubid, Relation relation,
                                         List *ancestors, bool pubviaroot) {
    Oid relid = RelationGetRelid(relation);
    Oid publish_as_relid = relid;

    // For partitions with pubviaroot, find the topmost ancestor
    if (pubviaroot && relation->rd_rel->relispartition) {
        publish_as_relid = GetTopMostAncestorInPublication(pubid, ancestors, NULL);
        if (!OidIsValid(publish_as_relid))
            publish_as_relid = relid;
    }

    // Look up the column list for this publication-relation pair
    HeapTuple tuple = SearchSysCache2(PUBLICATIONRELMAP,
                                     ObjectIdGetDatum(publish_as_relid),
                                     ObjectIdGetDatum(pubid));
    if (!HeapTupleIsValid(tuple))
        return false;

    Datum datum;
    bool isnull;
    datum = SysCacheGetAttr(PUBLICATIONRELMAP, tuple,
                           Anum_pg_publication_rel_prattrs, &isnull);

    bool result = false;
    if (!isnull) {
        // REPLICA IDENTITY FULL is incompatible with column lists
        if (relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL) {
            result = true;
        } else {
            // Check if all replica identity columns are in the column list
            Bitmapset *columns = pub_collist_to_bitmapset(NULL, datum, NULL);
            Bitmapset *idattrs = RelationGetIndexAttrBitmap(relation,
                                                          INDEX_ATTR_BITMAP_IDENTITY_KEY);

            // Verify each replica identity column is covered
            int x = -1;
            while ((x = bms_next_member(idattrs, x)) >= 0) {
                AttrNumber attnum = (x + FirstLowInvalidHeapAttributeNumber);

                // Handle pubviaroot column mapping
                if (pubviaroot) {
                    char *colname = get_attname(relid, attnum, false);
                    attnum = get_attnum(publish_as_relid, colname);
                }

                if (!bms_is_member(attnum, columns)) {
                    result = true;
                    break;
                }
            }

            bms_free(idattrs);
            bms_free(columns);
        }
    }

    ReleaseSysCache(tuple);
    return result;
}
```