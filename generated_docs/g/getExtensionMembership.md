# getExtensionMembership

## Location
[src/bin/pg_dump/pg_dump.c:18271-18363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18271-L18363)

## Overview
Obtains extension membership data from the PostgreSQL catalogs to identify objects that belong to extensions, enabling pg_dump to correctly determine whether they need to be dumped individually or will be recreated by CREATE EXTENSION commands.

## Definition
```c
void getExtensionMembership(Archive *fout, ExtensionInfo extinfo[], int numExtensions)
```

## Detailed Description
This function queries the pg_depend catalog to find all objects that are members of extensions. Extension member objects are typically not dumped individually since they will be recreated by the CREATE EXTENSION command. However, in binary upgrade mode, these members still need to be dumped individually.

The function executes a SQL query to retrieve dependency information where:
- refclassid = 'pg_extension'::regclass (references extension objects)
- deptype = 'e' (extension dependency type)

Results are ordered by referenced object ID to optimize processing when multiple objects belong to the same extension. For each dependency found, it calls recordExtensionMembership() to mark the object as an extension member.

## Parameters / Member Variables
- `fout`: Archive context for the dump operation
- `extinfo[]`: Array of ExtensionInfo structures containing extension metadata
- `numExtensions`: Number of extensions in the extinfo array

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - atooid
  - [findExtensionByOid](../f/findExtensionByOid.md)
  - [recordExtensionMembership](../r/recordExtensionMembership.md)
  - pg_log_warning
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from:
  - [getSchemaData](getSchemaData.md) (in src/bin/pg_dump/common.c:140)

## Notes and Other Information
- Early termination if numExtensions is 0 for efficiency
- Uses ordered results to minimize extension lookups when processing multiple objects from the same extension
- Handles cases where referenced extensions cannot be found with warning messages
- Critical for proper extension handling in both normal and binary upgrade dump modes
- The query uses a redundant refclassid constraint that may improve search performance

## Simplified Source

```c
void
getExtensionMembership(Archive *fout, ExtensionInfo extinfo[], int numExtensions)
{
    PQExpBuffer query;
    PGresult *res;
    int ntups, i;
    int i_classid, i_objid, i_refobjid;
    ExtensionInfo *ext;

    // Early return if no extensions
    if (numExtensions == 0)
        return;

    query = createPQExpBuffer();

    // Query extension dependencies from pg_depend
    appendPQExpBufferStr(query, "SELECT "
                               "classid, objid, refobjid "
                               "FROM pg_depend "
                               "WHERE refclassid = 'pg_extension'::regclass "
                               "AND deptype = 'e' "
                               "ORDER BY 3");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    i_classid = PQfnumber(res, "classid");
    i_objid = PQfnumber(res, "objid");
    i_refobjid = PQfnumber(res, "refobjid");

    // Process dependencies, ordered by extension for efficiency
    ext = NULL;

    for (i = 0; i < ntups; i++) {
        CatalogId objId;
        Oid extId;

        objId.tableoid = atooid(PQgetvalue(res, i, i_classid));
        objId.oid = atooid(PQgetvalue(res, i, i_objid));
        extId = atooid(PQgetvalue(res, i, i_refobjid));

        // Find extension (cache for efficiency)
        if (ext == NULL || ext->dobj.catId.oid != extId)
            ext = findExtensionByOid(extId);

        if (ext == NULL) {
            // Extension not found - log warning and continue
            pg_log_warning("could not find referenced extension %u", extId);
            continue;
        }

        // Record this object as a member of the extension
        recordExtensionMembership(objId, ext);
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```