# CreatePublication

## Location
[src/backend/commands/publicationcmds.c:728-870](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L728-L870)

## Overview
CreatePublication creates a new logical replication publication in PostgreSQL, which defines a set of tables or schemas whose changes can be replicated to subscribers.

## Definition

```c
ObjectAddress
CreatePublication(ParseState *pstate, CreatePublicationStmt *stmt)
```
## Detailed Description
CreatePublication is the core function responsible for creating a new publication object in PostgreSQL's logical replication system. It performs comprehensive validation, creates the catalog entry, and associates the specified tables or schemas with the publication. The function handles both explicit table lists and schema-based publications, with special handling for "FOR ALL TABLES" publications that require superuser privileges.

The function validates permissions (requiring CREATE privilege on the database and superuser for certain publication types), ensures unique publication names, parses publication options, creates the catalog tuple, and establishes object dependencies. It also handles the association of tables and schemas with the publication, including WHERE clause transformation and column list validation.

## Parameters / Member Variables
- `*pstate`: ParseState containing parsing context and source text information
- `*stmt`: CreatePublicationStmt structure containing the publication creation command details including name, options, and object specifications
## Dependencies
- Functions called/Symbols referenced:
  - [object_aclcheck](../o/object_aclcheck.md): Permission checking for database CREATE privilege
  - [parse_publication_options](../p/parse_publication_options.md): Parses publication-specific options like publish actions
  - [ObjectsInPublicationToOids](../O/ObjectsInPublicationToOids.md): Converts publication objects to relation and schema OID lists
  - [TransformPubWhereClauses](../T/TransformPubWhereClauses.md): Processes WHERE clauses for publication relations
  - [CheckPubRelationColumnList](CheckPubRelationColumnList.md): Validates column specifications for publication relations
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md): Records ownership dependency for the publication
  - [heap_form_tuple](../h/heap_form_tuple.md)/heap_freetuple: Tuple creation and cleanup
  - [CatalogTupleInsert](CatalogTupleInsert.md): Inserts the publication tuple into pg_publication catalog
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processing function

## Notes and Other Information
- Requires CREATE privilege on the database for basic publications
- FOR ALL TABLES and FOR TABLES IN SCHEMA publications require superuser privileges
- Issues a WARNING if wal_level is not set to logical, as this is required for logical replication
- Invalidates relation cache for FOR ALL TABLES publications to rebuild publication information
- Supports both explicit table lists and schema-based table inclusion
- Handles partition root publishing preferences through publish_via_partition_root option

## Simplified Source

```c
ObjectAddress
CreatePublication(ParseState *pstate, CreatePublicationStmt *stmt)
{
    Relation rel;
    ObjectAddress myself;
    Oid puboid;
    bool nulls[Natts_pg_publication];
    Datum values[Natts_pg_publication];
    HeapTuple tup;
    PublicationActions pubactions;
    bool publish_via_partition_root;
    List *relations = NIL;
    List *schemaidlist = NIL;

    // Check CREATE privilege on database
    AclResult aclresult = object_aclcheck(DatabaseRelationId, MyDatabaseId, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_DATABASE, get_database_name(MyDatabaseId));

    // FOR ALL TABLES requires superuser
    if (stmt->for_all_tables && !superuser())
        ereport(ERROR, "must be superuser to create FOR ALL TABLES publication");

    // Open publication catalog
    rel = table_open(PublicationRelationId, RowExclusiveLock);

    // Check if publication name already exists
    puboid = GetSysCacheOid1(PUBLICATIONNAME, Anum_pg_publication_oid,
                             CStringGetDatum(stmt->pubname));
    if (OidIsValid(puboid))
        ereport(ERROR, "publication already exists");

    // Prepare tuple data
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    // Set basic publication attributes
    values[Anum_pg_publication_pubname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum(stmt->pubname));
    values[Anum_pg_publication_pubowner - 1] = ObjectIdGetDatum(GetUserId());

    // Parse publication options (publish actions, partition root setting)
    parse_publication_options(pstate, stmt->options,
                             &publish_given, &pubactions,
                             &publish_via_partition_root_given,
                             &publish_via_partition_root);

    // Assign new OID and set publication properties
    puboid = GetNewOidWithIndex(rel, PublicationObjectIndexId, Anum_pg_publication_oid);
    values[Anum_pg_publication_oid - 1] = ObjectIdGetDatum(puboid);
    values[Anum_pg_publication_puballtables - 1] = BoolGetDatum(stmt->for_all_tables);
    values[Anum_pg_publication_pubinsert - 1] = BoolGetDatum(pubactions.pubinsert);
    values[Anum_pg_publication_pubupdate - 1] = BoolGetDatum(pubactions.pubupdate);
    values[Anum_pg_publication_pubdelete - 1] = BoolGetDatum(pubactions.pubdelete);
    values[Anum_pg_publication_pubtruncate - 1] = BoolGetDatum(pubactions.pubtruncate);
    values[Anum_pg_publication_pubviaroot - 1] = BoolGetDatum(publish_via_partition_root);

    // Create and insert tuple
    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    // Record dependency on owner
    recordDependencyOnOwner(PublicationRelationId, puboid, GetUserId());

    ObjectAddressSet(myself, PublicationRelationId, puboid);
    CommandCounterIncrement();

    // Associate objects with publication
    if (stmt->for_all_tables) {
        // Invalidate relcache for all tables
        CacheInvalidateRelcacheAll();
    } else {
        // Process specific tables and schemas
        ObjectsInPublicationToOids(stmt->pubobjects, pstate, &relations, &schemaidlist);

        // FOR TABLES IN SCHEMA requires superuser
        if (schemaidlist != NIL && !superuser())
            ereport(ERROR, "must be superuser to create FOR TABLES IN SCHEMA publication");

        // Add specified tables
        if (relations != NIL) {
            List *rels = OpenTableList(relations);
            TransformPubWhereClauses(rels, pstate->p_sourcetext, publish_via_partition_root);
            CheckPubRelationColumnList(stmt->pubname, rels,
                                      schemaidlist != NIL, publish_via_partition_root);
            PublicationAddTables(puboid, rels, true, NULL);
            CloseTableList(rels);
        }

        // Add specified schemas
        if (schemaidlist != NIL) {
            LockSchemaList(schemaidlist);
            PublicationAddSchemas(puboid, schemaidlist, true, NULL);
        }
    }

    table_close(rel, RowExclusiveLock);
    InvokeObjectPostCreateHook(PublicationRelationId, puboid, 0);

    // Warn if wal_level is insufficient
    if (wal_level != WAL_LEVEL_LOGICAL)
        ereport(WARNING, "wal_level is insufficient to publish logical changes");

    return myself;
}
```