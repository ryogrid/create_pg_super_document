# getAggregates

## Location
[src/bin/pg_dump/pg_dump.c:6460-6606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6460-L6606)

## Overview
The getAggregates function retrieves all user-defined aggregate functions from the PostgreSQL system catalogs and returns them in an AggInfo structure array for use by pg_dump.

## Definition

```c
AggInfo *
getAggregates(Archive *fout, int *numAggs)
```
## Detailed Description
This function is part of pg_dump's catalog reading functionality that specifically handles aggregate functions. It constructs and executes a complex SQL query against the pg_proc system catalog to retrieve user-defined aggregates, filtering out system-defined aggregates in pg_catalog unless they have custom privileges. The function handles different PostgreSQL versions, using different aggregate identification methods (proisagg for older versions, prokind = 'a' for PostgreSQL 11+).

The function creates AggInfo structures for each aggregate, populating them with comprehensive metadata including OID, name, namespace, argument types, owner, and access control information. It also handles argument type parsing for aggregates that take parameters and manages ACL (Access Control List) information for privilege management during dump/restore operations.

## Parameters / Member Variables
- `*fout`: Archive structure containing connection information and dump configuration options
- `*numAggs`: Pointer to integer that will be set to the number of aggregates found
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - atoi
  - [parseOidArray](../p/parseOidArray.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- The function uses version-specific SQL queries to handle changes in PostgreSQL's aggregate identification (proisagg vs prokind)
- System aggregates in pg_catalog are filtered out unless they have custom privileges or are part of extensions during binary upgrades
- ACL information is preserved for aggregates that have custom privileges
- Argument types are parsed from the proargtypes array and stored as separate Oid arrays
- The function handles both parameterized and non-parameterized aggregates
- Uses DO_AGG object type identifier for dump object classification
- Supports binary upgrade mode with special handling for extension-dependent aggregates

## Simplified Source

```c
AggInfo *
getAggregates(Archive *fout, int *numAggs)
{
    DumpOptions *dopt = fout->dopt;
    PGresult   *res;
    int         ntups;
    int         i;
    PQExpBuffer query = createPQExpBuffer();
    AggInfo    *agginfo;

    // Build version-specific query to find user-defined aggregates
    if (fout->remoteVersion >= 90600)
    {
        // Use prokind for PostgreSQL 11+, proisagg for older versions
        const char *agg_check = (fout->remoteVersion >= 110000 ? "p.prokind = 'a'" : "p.proisagg");

        appendPQExpBuffer(query, "SELECT p.tableoid, p.oid, "
                                "p.proname AS aggname, "
                                "p.pronamespace AS aggnamespace, "
                                "p.pronargs, p.proargtypes, "
                                "p.proowner, "
                                "p.proacl AS aggacl, "
                                "acldefault('f', p.proowner) AS acldefault "
                                "FROM pg_proc p "
                                "LEFT JOIN pg_init_privs pip ON "
                                "(p.oid = pip.objoid "
                                "AND pip.classoid = 'pg_proc'::regclass "
                                "AND pip.objsubid = 0) "
                                "WHERE %s AND ("
                                "p.pronamespace != "
                                "(SELECT oid FROM pg_namespace "
                                "WHERE nspname = 'pg_catalog') OR "
                                "p.proacl IS DISTINCT FROM pip.initprivs",
                                agg_check);

        // Add extension dependency check for binary upgrades
        if (dopt->binary_upgrade)
            appendPQExpBufferStr(query, " OR EXISTS(SELECT 1 FROM pg_depend WHERE "
                                       "classid = 'pg_proc'::regclass AND "
                                       "objid = p.oid AND "
                                       "refclassid = 'pg_extension'::regclass AND "
                                       "deptype = 'e')");
        appendPQExpBufferChar(query, ')');
    }
    else
    {
        // Legacy query for older PostgreSQL versions
        appendPQExpBufferStr(query, "SELECT tableoid, oid, proname AS aggname, "
                                   "pronamespace AS aggnamespace, "
                                   "pronargs, proargtypes, "
                                   "proowner, "
                                   "proacl AS aggacl, "
                                   "acldefault('f', proowner) AS acldefault "
                                   "FROM pg_proc p "
                                   "WHERE proisagg AND ("
                                   "pronamespace != "
                                   "(SELECT oid FROM pg_namespace "
                                   "WHERE nspname = 'pg_catalog')");

        if (dopt->binary_upgrade)
            appendPQExpBufferStr(query, " OR EXISTS(SELECT 1 FROM pg_depend WHERE "
                                       "classid = 'pg_proc'::regclass AND "
                                       "objid = p.oid AND "
                                       "refclassid = 'pg_extension'::regclass AND "
                                       "deptype = 'e')");
        appendPQExpBufferChar(query, ')');
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numAggs = ntups;

    // Allocate array for aggregate info
    agginfo = (AggInfo *) pg_malloc(ntups * sizeof(AggInfo));

    // Get column indices for result fields
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_aggname = PQfnumber(res, "aggname");
    int i_aggnamespace = PQfnumber(res, "aggnamespace");
    int i_pronargs = PQfnumber(res, "pronargs");
    int i_proargtypes = PQfnumber(res, "proargtypes");
    int i_proowner = PQfnumber(res, "proowner");
    int i_aggacl = PQfnumber(res, "aggacl");
    int i_acldefault = PQfnumber(res, "acldefault");

    // Process each aggregate result
    for (i = 0; i < ntups; i++)
    {
        // Set object type and catalog info
        agginfo[i].aggfn.dobj.objType = DO_AGG;
        agginfo[i].aggfn.dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        agginfo[i].aggfn.dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));

        // Assign dump ID and basic properties
        AssignDumpId(&agginfo[i].aggfn.dobj);
        agginfo[i].aggfn.dobj.name = pg_strdup(PQgetvalue(res, i, i_aggname));
        agginfo[i].aggfn.dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_aggnamespace)));

        // Set ACL and role information
        agginfo[i].aggfn.dacl.acl = pg_strdup(PQgetvalue(res, i, i_aggacl));
        agginfo[i].aggfn.dacl.acldefault = pg_strdup(PQgetvalue(res, i, i_acldefault));
        agginfo[i].aggfn.dacl.privtype = 0;
        agginfo[i].aggfn.dacl.initprivs = NULL;
        agginfo[i].aggfn.rolname = getRoleName(PQgetvalue(res, i, i_proowner));

        // Initialize function properties
        agginfo[i].aggfn.lang = InvalidOid;
        agginfo[i].aggfn.prorettype = InvalidOid;
        agginfo[i].aggfn.nargs = atoi(PQgetvalue(res, i, i_pronargs));

        // Parse argument types if aggregate has parameters
        if (agginfo[i].aggfn.nargs == 0)
            agginfo[i].aggfn.argtypes = NULL;
        else
        {
            agginfo[i].aggfn.argtypes = (Oid *) pg_malloc(agginfo[i].aggfn.nargs * sizeof(Oid));
            parseOidArray(PQgetvalue(res, i, i_proargtypes),
                         agginfo[i].aggfn.argtypes,
                         agginfo[i].aggfn.nargs);
        }

        agginfo[i].aggfn.postponed_def = false;

        // Determine if this aggregate should be dumped
        selectDumpableObject(&(agginfo[i].aggfn.dobj), fout);

        // Mark whether aggregate has an ACL
        if (!PQgetisnull(res, i, i_aggacl))
            agginfo[i].aggfn.dobj.components |= DUMP_COMPONENT_ACL;
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    return agginfo;
}
```