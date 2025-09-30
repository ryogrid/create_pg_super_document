# publication_add_schema

## Location
[src/backend/catalog/pg_publication.c:606-686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L606-L686)

## Overview
Inserts a new publication/schema mapping into the pg_publication_namespace catalog table, establishing a relationship between a publication and a schema.

## Definition
```c
ObjectAddress publication_add_schema(Oid pubid, Oid schemaid, bool if_not_exists)
```

## Detailed Description
This function creates a new entry in the pg_publication_namespace system catalog to associate a schema with a publication. When a schema is added to a publication, all existing and future tables in that schema become part of the publication automatically. The function handles duplicate checking, catalog entry creation, dependency establishment, and cache invalidation.

The function performs several key operations:
1. Checks for existing schema-publication mappings and handles them based on the if_not_exists parameter
2. Validates that the schema can be added to the publication via check_publication_add_schema
3. Creates a new catalog tuple with the publication ID, schema ID, and a new OID for the mapping entry
4. Establishes dependency relationships between the mapping entry and both the publication and schema
5. Invalidates relation caches for all tables in the schema to ensure publication information is properly updated

Similar to publication_add_relation, this function considers partition hierarchies when invalidating caches, ensuring that partitioned tables and their partitions are properly handled.

## Parameters / Member Variables
- `pubid`: Object ID of the publication to which the schema should be added
- `schemaid`: Object ID of the schema to be added to the publication
- `if_not_exists`: Boolean flag indicating whether to silently skip if the schema is already a member of the publication

## Dependencies
- Functions called/Symbols referenced:
  - [GetPublication](../G/GetPublication.md)
  - SearchSysCacheExists2
  - [get_namespace_name](../g/get_namespace_name.md)
  - [check_publication_add_schema](../c/check_publication_add_schema.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [GetSchemaPublicationRelations](../G/GetSchemaPublicationRelations.md)
  - [InvalidatePublicationRels](../I/InvalidatePublicationRels.md)
- Called from (representative examples):
  - [PublicationAddSchemas](../P/PublicationAddSchemas.md) (src/backend/commands/publicationcmds.c:1838)

## Notes and Other Information
- The function uses RowExclusiveLock on the pg_publication_namespace catalog to ensure consistency
- Duplicate detection provides better error messages, but real protection comes from the unique key constraint on the catalog
- Dependencies are established with DEPENDENCY_AUTO type for both the publication and schema relationships
- Cache invalidation considers all tables in the schema, including partitioned tables and their partitions
- The function returns an ObjectAddress pointing to the newly created pg_publication_namespace entry
- Schema-level publications automatically include all tables in the schema, both existing and future ones
- Used primarily by DDL commands like ALTER PUBLICATION ADD TABLES IN SCHEMA
- Location: src/backend/catalog/pg_publication.c:606-686

## Simplified Source

```c
ObjectAddress
publication_add_schema(Oid pubid, Oid schemaid, bool if_not_exists)
{
    Publication *pub = GetPublication(pubid);
    ObjectAddress myself, referenced;

    // Open catalog with exclusive lock
    Relation rel = table_open(PublicationNamespaceRelationId, RowExclusiveLock);

    // Check for existing mapping
    if (SearchSysCacheExists2(PUBLICATIONNAMESPACEMAP, ObjectIdGetDatum(schemaid), ObjectIdGetDatum(pubid))) {
        table_close(rel, RowExclusiveLock);
        if (if_not_exists)
            return InvalidObjectAddress;
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                       errmsg("schema \"%s\" is already member of publication \"%s\"",
                             get_namespace_name(schemaid), pub->name)));
    }

    // Validate schema can be added
    check_publication_add_schema(schemaid);

    // Create new catalog entry
    Datum values[Natts_pg_publication_namespace];
    bool nulls[Natts_pg_publication_namespace];
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    Oid psschid = GetNewOidWithIndex(rel, PublicationNamespaceObjectIndexId, Anum_pg_publication_namespace_oid);
    values[Anum_pg_publication_namespace_oid - 1] = ObjectIdGetDatum(psschid);
    values[Anum_pg_publication_namespace_pnpubid - 1] = ObjectIdGetDatum(pubid);
    values[Anum_pg_publication_namespace_pnnspid - 1] = ObjectIdGetDatum(schemaid);

    HeapTuple tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    // Set up object addresses and dependencies
    ObjectAddressSet(myself, PublicationNamespaceRelationId, psschid);
    ObjectAddressSet(referenced, PublicationRelationId, pubid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
    ObjectAddressSet(referenced, NamespaceRelationId, schemaid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    table_close(rel, RowExclusiveLock);

    // Invalidate relcache for all schema relations
    List *schemaRels = GetSchemaPublicationRelations(schemaid, PUBLICATION_PART_ALL);
    InvalidatePublicationRels(schemaRels);

    return myself;
}
```