# publication_add_relation

## Location
[src/backend/catalog/pg_publication.c:358-480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L358-L480)

## Overview
Inserts a new publication/relation mapping into the pg_publication_rel catalog table, establishing a relationship between a publication and a table with optional column list and WHERE clause qualifications.

## Definition

```c
ObjectAddress
publication_add_relation(Oid pubid, PublicationRelInfo *pri,
						 bool if_not_exists)
```
## Detailed Description
This function creates a new entry in the pg_publication_rel system catalog to associate a relation (table) with a publication. It handles the complete process of validating the relation, translating column specifications, creating catalog entries, and establishing proper dependencies. The function supports conditional insertion (IF NOT EXISTS semantics) and can include row filtering through WHERE clauses and column filtering through explicit column lists.

The function performs several key operations:
1. Validates that the relation can be added to the publication
2. Checks for existing duplicates and handles them based on the if_not_exists parameter
3. Translates column names to attribute numbers and validates column specifications
4. Creates the catalog tuple with appropriate values for relation ID, publication ID, WHERE clause, and column list
5. Establishes dependency relationships between the publication entry and referenced objects
6. Invalidates relation caches to ensure publication information is properly updated

## Parameters / Member Variables
- `pubid`: Object ID of the publication to which the relation should be added
- `*pri`: PublicationRelInfo structure containing the relation, optional column list, and optional WHERE clause
- `if_not_exists`: Boolean flag indicating whether to silently skip if the relation is already a member of the publication
## Dependencies
- Functions called/Symbols referenced:
  - [GetPublication](../G/GetPublication.md)
  - SearchSysCacheExists2
  - [check_publication_add_relation](../c/check_publication_add_relation.md)
  - [publication_translate_columns](publication_translate_columns.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [nodeToString](../n/nodeToString.md)
  - [buildint2vector](../b/buildint2vector.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnSingleRelExpr](../r/recordDependencyOnSingleRelExpr.md)
  - ObjectAddressSubSet
  - [GetPubPartitionOptionRelations](../G/GetPubPartitionOptionRelations.md)
  - [InvalidatePublicationRels](../I/InvalidatePublicationRels.md)
- Called from (representative examples):
  - [PublicationAddTables](../P/PublicationAddTables.md) (src/backend/commands/publicationcmds.c:1765)

## Notes and Other Information
- The function uses RowExclusiveLock on the pg_publication_rel catalog to ensure consistency
- Duplicate detection is performed for better error messages, but the real protection comes from the unique key constraint on the catalog
- For partitioned tables, the function invalidates all partitions in the hierarchy since child tables are implicitly published when parent tables are published
- Column list validation ensures only allowed columns (no system or generated columns) are included
- Dependencies are established not only on the publication and relation, but also on individual columns and objects referenced in WHERE clauses
- Returns an ObjectAddress pointing to the newly created pg_publication_rel entry
- Location: src/backend/catalog/pg_publication.c:358-480

## Simplified Source

```c
ObjectAddress
publication_add_relation(Oid pubid, PublicationRelInfo *pri, bool if_not_exists)
{
    Relation targetrel = pri->relation;
    Oid relid = RelationGetRelid(targetrel);
    Publication *pub = GetPublication(pubid);
    ObjectAddress myself, referenced;

    // Open catalog with exclusive lock
    Relation rel = table_open(PublicationRelRelationId, RowExclusiveLock);

    // Check for existing mapping
    if (SearchSysCacheExists2(PUBLICATIONRELMAP, ObjectIdGetDatum(relid), ObjectIdGetDatum(pubid))) {
        table_close(rel, RowExclusiveLock);
        if (if_not_exists)
            return InvalidObjectAddress;
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                       errmsg("relation \"%s\" is already member of publication \"%s\"",
                             RelationGetRelationName(targetrel), pub->name)));
    }

    // Validate relation and translate column list
    check_publication_add_relation(targetrel);
    AttrNumber *attarray = NULL;
    int natts = 0;
    publication_translate_columns(pri->relation, pri->columns, &natts, &attarray);

    // Create catalog entry
    Datum values[Natts_pg_publication_rel];
    bool nulls[Natts_pg_publication_rel];
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    Oid pubreloid = GetNewOidWithIndex(rel, PublicationRelObjectIndexId, Anum_pg_publication_rel_oid);
    values[Anum_pg_publication_rel_oid - 1] = ObjectIdGetDatum(pubreloid);
    values[Anum_pg_publication_rel_prpubid - 1] = ObjectIdGetDatum(pubid);
    values[Anum_pg_publication_rel_prrelid - 1] = ObjectIdGetDatum(relid);

    // Add WHERE clause and column list if specified
    if (pri->whereClause != NULL)
        values[Anum_pg_publication_rel_prqual - 1] = CStringGetTextDatum(nodeToString(pri->whereClause));
    else
        nulls[Anum_pg_publication_rel_prqual - 1] = true;

    if (pri->columns)
        values[Anum_pg_publication_rel_prattrs - 1] = PointerGetDatum(buildint2vector(attarray, natts));
    else
        nulls[Anum_pg_publication_rel_prattrs - 1] = true;

    HeapTuple tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    // Set up dependencies
    ObjectAddressSet(myself, PublicationRelRelationId, pubreloid);
    ObjectAddressSet(referenced, PublicationRelationId, pubid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
    ObjectAddressSet(referenced, RelationRelationId, relid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    // Add dependencies for WHERE clause and columns
    if (pri->whereClause)
        recordDependencyOnSingleRelExpr(&myself, pri->whereClause, relid, DEPENDENCY_NORMAL, DEPENDENCY_NORMAL, false);
    for (int i = 0; i < natts; i++) {
        ObjectAddressSubSet(referenced, RelationRelationId, relid, attarray[i]);
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    table_close(rel, RowExclusiveLock);

    // Invalidate caches for relation and all partitions
    List *relids = GetPubPartitionOptionRelations(NIL, PUBLICATION_PART_ALL, relid);
    InvalidatePublicationRels(relids);

    return myself;
}
```