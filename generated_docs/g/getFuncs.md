# getFuncs

## Location
[src/bin/pg_dump/pg_dump.c:6607-6805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6607-L6805)

## Overview
The getFuncs function retrieves all user-defined functions from the PostgreSQL system catalogs and returns them in a FuncInfo structure array for use by pg_dump, excluding aggregates and internally dependent functions.

## Definition

```c
structor
	 * functions for range types.  Note this is OK only because the
	 * constructors don't have any dependencies the range type doesn't have;
```
## Detailed Description
This function is part of pg_dump's catalog reading functionality that handles regular functions (non-aggregates). It implements sophisticated filtering logic to determine which functions should be included in the dump. The function constructs complex SQL queries that vary based on PostgreSQL version, filtering out aggregates, internally dependent functions (like range type constructors), and system functions in pg_catalog unless they meet specific criteria.

The filtering criteria include functions used by casts or transforms, functions that are part of extensions in binary-upgrade mode, and functions with custom privileges different from their initial privileges. The function handles version-specific changes in PostgreSQL's function identification (proisagg vs prokind) and supports both older and newer privilege management systems.

Each function is represented by a FuncInfo structure containing comprehensive metadata including OID, name, namespace, language, argument types, return type, owner, and access control information.

## Parameters / Member Variables
- : Archive structure containing connection information and dump configuration options
- : Pointer to integer that will be set to the number of functions found

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [pg_malloc0](../p/pg_malloc0.md)
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
- Excludes aggregate functions (handled separately by getAggregates)
- Filters out internally dependent functions like range type constructors
- System functions in pg_catalog are excluded unless they meet special criteria (used by casts/transforms, extension members, custom privileges)
- Uses version-specific SQL queries to handle PostgreSQL evolution (proisagg vs prokind field)
- Supports binary upgrade mode with special handling for extension-dependent functions
- Handles both parameterized and non-parameterized functions with proper argument type parsing
- Uses DO_FUNC object type identifier for dump object classification
- Preserves ACL information for functions with custom privileges
- Memory allocation uses pg_malloc0 for zero-initialized structures

## Simplified Source

```c
FuncInfo *
getFuncs(Archive *fout, int *numFuncs)
{
    DumpOptions *dopt = fout->dopt;
    PGresult *res;
    int ntups, i;
    PQExpBuffer query = createPQExpBuffer();
    FuncInfo *finfo;
    int i_tableoid, i_oid, i_proname, i_pronamespace, i_proowner;
    int i_prolang, i_pronargs, i_proargtypes, i_prorettype, i_proacl, i_acldefault;

    // Build version-specific query to find interesting functions
    if (fout->remoteVersion >= 90600) {
        const char *not_agg_check = (fout->remoteVersion >= 110000 ? "p.prokind <> 'a'" : "NOT p.proisagg");

        appendPQExpBuffer(query,
                          "SELECT p.tableoid, p.oid, p.proname, p.prolang, "
                          "p.pronargs, p.proargtypes, p.prorettype, "
                          "p.proacl, acldefault('f', p.proowner) AS acldefault, "
                          "p.pronamespace, p.proowner "
                          "FROM pg_proc p "
                          "LEFT JOIN pg_init_privs pip ON (p.oid = pip.objoid "
                          "AND pip.classoid = 'pg_proc'::regclass AND pip.objsubid = 0) "
                          "WHERE %s "
                          "AND NOT EXISTS (SELECT 1 FROM pg_depend "
                          "WHERE classid = 'pg_proc'::regclass AND objid = p.oid AND deptype = 'i') "
                          "AND (pronamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog') "
                          "OR EXISTS (SELECT 1 FROM pg_cast WHERE pg_cast.oid > %u AND p.oid = pg_cast.castfunc) "
                          "OR EXISTS (SELECT 1 FROM pg_transform WHERE pg_transform.oid > %u "
                          "AND (p.oid = pg_transform.trffromsql OR p.oid = pg_transform.trftosql))",
                          not_agg_check, g_last_builtin_oid, g_last_builtin_oid);

        if (dopt->binary_upgrade)
            appendPQExpBufferStr(query, " OR EXISTS(SELECT 1 FROM pg_depend WHERE "
                                 "classid = 'pg_proc'::regclass AND objid = p.oid AND "
                                 "refclassid = 'pg_extension'::regclass AND deptype = 'e')");

        appendPQExpBufferStr(query, " OR p.proacl IS DISTINCT FROM pip.initprivs)");
    } else {
        // Simplified query for older versions
        appendPQExpBuffer(query,
                          "SELECT tableoid, oid, proname, prolang, pronargs, proargtypes, "
                          "prorettype, proacl, acldefault('f', proowner) AS acldefault, "
                          "pronamespace, proowner FROM pg_proc p "
                          "WHERE NOT proisagg AND NOT EXISTS (SELECT 1 FROM pg_depend "
                          "WHERE classid = 'pg_proc'::regclass AND objid = p.oid AND deptype = 'i') "
                          "AND (pronamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog') "
                          "OR EXISTS (SELECT 1 FROM pg_cast WHERE pg_cast.oid > '%u'::oid "
                          "AND p.oid = pg_cast.castfunc))", g_last_builtin_oid);

        // Add additional conditions for newer versions and binary upgrade mode...
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numFuncs = ntups;

    // Allocate and populate function info array
    finfo = (FuncInfo *) pg_malloc0(ntups * sizeof(FuncInfo));

    // Get column indices
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_proname = PQfnumber(res, "proname");
    // ... other column indices

    // Process each function
    for (i = 0; i < ntups; i++) {
        finfo[i].dobj.objType = DO_FUNC;
        finfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        finfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&finfo[i].dobj);
        finfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_proname));
        finfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_pronamespace)));

        // Set ACL and other properties
        finfo[i].dacl.acl = pg_strdup(PQgetvalue(res, i, i_proacl));
        finfo[i].rolname = getRoleName(PQgetvalue(res, i, i_proowner));
        finfo[i].lang = atooid(PQgetvalue(res, i, i_prolang));
        finfo[i].nargs = atoi(PQgetvalue(res, i, i_pronargs));

        // Parse argument types if any
        if (finfo[i].nargs > 0) {
            finfo[i].argtypes = (Oid *) pg_malloc(finfo[i].nargs * sizeof(Oid));
            parseOidArray(PQgetvalue(res, i, i_proargtypes), finfo[i].argtypes, finfo[i].nargs);
        }

        // Determine if function should be dumped
        selectDumpableObject(&(finfo[i].dobj), fout);

        // Mark ACL component if present
        if (!PQgetisnull(res, i, i_proacl))
            finfo[i].dobj.components |= DUMP_COMPONENT_ACL;
    }

    PQclear(res);
    destroyPQExpBuffer(query);
    return finfo;
}
```