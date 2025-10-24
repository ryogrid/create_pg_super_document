# collectComments

## Location
[src/bin/pg_dump/pg_dump.c:10437-10521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L10437-L10521)

## Overview
Constructs a table of all comments available for database objects and sets the has-comment component flag for each relevant object in pg_dump.

## Definition

```c
struct lookup table containing OIDs in numeric form */

	i_description = PQfnumber(res, "description");
```
## Detailed Description
The  function retrieves all comments from the  system catalog in a single query and builds an in-memory lookup table. This approach is much more efficient than performing per-object queries for comments. The function also sets the  flag on dumpable objects that have associated comments, which helps the dump process know which objects require comment handling.

The collected comments are stored in a global array  sorted by  for fast lookup during the dump process. The function handles a special case for composite types where column comments are linked to the type's pg_class entry but the flag needs to be set on the type's own DumpableObject.

## Parameters / Member Variables
- : Archive structure representing the dump destination and containing connection information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [findObjectByCatalogId](../f/findObjectByCatalogId.md)
  - [findTypeByOid](../f/findTypeByOid.md)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [createPQExpBuffer](createPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c:991)

## Notes and Other Information
- Uses a single SQL query to fetch all comments at once for performance optimization
- Results are sorted by classoid, objoid, objsubid to enable efficient binary search lookups
- Special handling for composite type column comments that need flag propagation to the parent type
- Memory allocation is done upfront for the entire comments array based on query result count
- Only comments for dumpable objects are retained in the final comments array

## Simplified Source

```c
static void collectComments(Archive *fout) {
    PGresult *res;
    PQExpBuffer query;
    int i_description, i_classoid, i_objoid, i_objsubid;
    int ntups, i;
    DumpableObject *dobj;

    query = createPQExpBuffer();

    // Query all comments ordered for binary search
    appendPQExpBufferStr(query, "SELECT description, classoid, objoid, objsubid "
                                "FROM pg_catalog.pg_description "
                                "ORDER BY classoid, objoid, objsubid");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    // Get column indices
    i_description = PQfnumber(res, "description");
    i_classoid = PQfnumber(res, "classoid");
    i_objoid = PQfnumber(res, "objoid");
    i_objsubid = PQfnumber(res, "objsubid");

    ntups = PQntuples(res);

    // Allocate global comments array
    comments = (CommentItem *) pg_malloc(ntups * sizeof(CommentItem));
    ncomments = 0;
    dobj = NULL;

    // Process each comment entry
    for (i = 0; i < ntups; i++) {
        CatalogId objId;
        int subid;

        objId.tableoid = atooid(PQgetvalue(res, i, i_classoid));
        objId.oid = atooid(PQgetvalue(res, i, i_objoid));
        subid = atoi(PQgetvalue(res, i, i_objsubid));

        // Find the dumpable object for this comment
        if (dobj == NULL ||
            dobj->catId.tableoid != objId.tableoid ||
            dobj->catId.oid != objId.oid)
            dobj = findObjectByCatalogId(objId);

        if (dobj == NULL)
            continue; // Skip comments for non-dumpable objects

        // Handle special case for composite type column comments
        if (subid != 0 && dobj->objType == DO_TABLE &&
            ((TableInfo *) dobj)->relkind == RELKIND_COMPOSITE_TYPE) {
            TypeInfo *cTypeInfo;

            cTypeInfo = findTypeByOid(((TableInfo *) dobj)->reltype);
            if (cTypeInfo)
                cTypeInfo->dobj.components |= DUMP_COMPONENT_COMMENT;
        } else {
            dobj->components |= DUMP_COMPONENT_COMMENT;
        }

        // Store comment in global array
        comments[ncomments].descr = pg_strdup(PQgetvalue(res, i, i_description));
        comments[ncomments].classoid = objId.tableoid;
        comments[ncomments].objoid = objId.oid;
        comments[ncomments].objsubid = subid;
        ncomments++;
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```