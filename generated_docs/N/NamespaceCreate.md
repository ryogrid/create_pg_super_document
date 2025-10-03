# NamespaceCreate

## Location
[src/backend/catalog/pg_namespace.c:43-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_namespace.c#L43-L120)

## Overview
Creates a new namespace (schema) in the PostgreSQL catalog with the given name and owner, handling both regular schemas and temporary schemas with appropriate dependency tracking.

## Definition

```c
Oid
NamespaceCreate(const char *nspName, Oid ownerId, bool isTemp)
```
## Detailed Description
NamespaceCreate is the core function responsible for creating new namespaces (schemas) in PostgreSQL's catalog system. It performs comprehensive validation, inserts the new namespace record into the pg_namespace system catalog, and establishes all necessary dependencies. The function handles both regular schemas and temporary schemas, with special treatment for temporary schemas to prevent them from being linked as extension members and to skip default ACL processing. The function ensures proper locking, generates a unique OID for the namespace, and invokes post-creation hooks for extensibility.

## Parameters / Member Variables
- `*nspName`: The name of the namespace to be created; must not be NULL and must be unique within the database
- `ownerId`: The OID of the role that will own the new namespace
- `isTemp`: Boolean flag indicating whether this is a temporary schema; affects extension membership and ACL processing
## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists1: Check for existing namespace with the same name
  - [get_user_default_acl](../g/get_user_default_acl.md): Retrieve default ACL for the schema owner (skipped for temp schemas)
  - [table_open](../t/table_open.md): Open the pg_namespace catalog for modification
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md): Generate a unique OID for the new namespace
  - [namestrcpy](../n/namestrcpy.md): Copy the namespace name into a NameData structure
  - [heap_form_tuple](../h/heap_form_tuple.md): Create the catalog tuple for insertion
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md): Insert the new namespace tuple into pg_namespace
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md): Record ownership dependency
  - [recordDependencyOnNewAcl](../r/recordDependencyOnNewAcl.md): Record ACL-related dependencies
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md): Record extension membership (skipped for temp schemas)
  - InvokeObjectPostCreateHook: Trigger post-creation hooks for extensions

- Called from (representative examples):
  - [InitTempTableNamespace](../I/InitTempTableNamespace.md): Creates temporary schemas for backend sessions
  - [CreateSchemaCommand](../C/CreateSchemaCommand.md): Implements the CREATE SCHEMA SQL command

## Notes and Other Information
- The function includes comprehensive error checking, ensuring the namespace name is provided and doesn't conflict with existing schemas
- Temporary schemas receive special handling: they are excluded from extension membership to prevent temporary tables created in extension scripts from making the temp schema part of the extension
- Default ACL processing is skipped for temporary schemas as it's not necessary for their intended use
- The function maintains proper catalog consistency through row-exclusive locking and transactional operations
- All dependency relationships are properly recorded to ensure cascade operations work correctly during schema drops
- Post-creation hooks allow extensions and other components to respond to schema creation events

## Simplified Source

```c
Oid
NamespaceCreate(const char *nspName, Oid ownerId, bool isTemp)
{
    Relation nspdesc;
    HeapTuple tup;
    Oid nspoid;
    bool nulls[Natts_pg_namespace];
    Datum values[Natts_pg_namespace];
    NameData nname;
    TupleDesc tupDesc;
    ObjectAddress myself;
    Acl *nspacl;

    // Validate namespace name
    if (!nspName)
        elog(ERROR, "no namespace name supplied");

    // Check for duplicate namespace
    if (SearchSysCacheExists1(NAMESPACENAME, PointerGetDatum(nspName)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_SCHEMA),
                       errmsg("schema \"%s\" already exists", nspName)));

    // Get default ACL (only for non-temp schemas)
    if (!isTemp)
        nspacl = get_user_default_acl(OBJECT_SCHEMA, ownerId, InvalidOid);
    else
        nspacl = NULL;

    // Open catalog and prepare tuple
    nspdesc = table_open(NamespaceRelationId, RowExclusiveLock);
    tupDesc = nspdesc->rd_att;

    // Initialize values array
    for (int i = 0; i < Natts_pg_namespace; i++)
    {
        nulls[i] = false;
        values[i] = (Datum) NULL;
    }

    // Set namespace attributes
    nspoid = GetNewOidWithIndex(nspdesc, NamespaceOidIndexId, Anum_pg_namespace_oid);
    values[Anum_pg_namespace_oid - 1] = ObjectIdGetDatum(nspoid);
    namestrcpy(&nname, nspName);
    values[Anum_pg_namespace_nspname - 1] = NameGetDatum(&nname);
    values[Anum_pg_namespace_nspowner - 1] = ObjectIdGetDatum(ownerId);

    if (nspacl != NULL)
        values[Anum_pg_namespace_nspacl - 1] = PointerGetDatum(nspacl);
    else
        nulls[Anum_pg_namespace_nspacl - 1] = true;

    // Insert into catalog
    tup = heap_form_tuple(tupDesc, values, nulls);
    CatalogTupleInsert(nspdesc, tup);
    table_close(nspdesc, RowExclusiveLock);

    // Record dependencies
    myself.classId = NamespaceRelationId;
    myself.objectId = nspoid;
    myself.objectSubId = 0;

    recordDependencyOnOwner(NamespaceRelationId, nspoid, ownerId);
    recordDependencyOnNewAcl(NamespaceRelationId, nspoid, 0, ownerId, nspacl);

    // Record extension dependency (except for temp schemas)
    if (!isTemp)
        recordDependencyOnCurrentExtension(&myself, false);

    // Invoke post-creation hooks
    InvokeObjectPostCreateHook(NamespaceRelationId, nspoid, 0);

    return nspoid;
}
```