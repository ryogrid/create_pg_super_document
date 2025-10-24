# getDefaultACLs

## Location
[src/bin/pg_dump/pg_dump.c:9846-9945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L9846-L9945)

## Overview
Reads all default ACL information from the system catalogs and returns them in a structured format for pg_dump processing.

## Definition
```c
DefaultACLInfo *getDefaultACLs(Archive *fout, int *numDefaultACLs)
```

## Detailed Description
This function is part of the pg_dump utility and extracts default access control list (ACL) information from the PostgreSQL system catalog `pg_default_acl`. Default ACLs define the default privileges that will be assigned to newly created objects of specific types. The function handles two types of default ACLs: global entries (with defaclnamespace=0) that replace hard-wired defaults, and namespace-specific entries that only add privileges. Global entries are dumped as deltas from the system default ACL, while namespace-specific entries are dumped as-is (deltas from an empty ACL). The function processes special handling for sequence objects by converting 'S' to 's' for the acldefault() function call.

## Parameters / Member Variables
- `fout`: Archive handle for the pg_dump operation, containing dump options and used for executing SQL queries
- `numDefaultACLs`: Output parameter that receives the count of default ACLs found

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)/PQfnumber/PQgetvalue
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - [selectDumpableDefaultACL](../s/selectDumpableDefaultACL.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- The function distinguishes between global default ACLs (defaclnamespace=0) and namespace-specific ACLs
- Global ACLs are computed as deltas from system defaults using the `acldefault()` function
- Namespace-specific ACLs use an empty ACL ({}) as their baseline
- Special case handling converts sequence object type 'S' to 's' for acldefault() compatibility
- Each default ACL automatically gets `DUMP_COMPONENT_ACL` since they are inherently ACL objects
- The object name is set to the defaclobjtype character for identification purposes
- Namespace resolution uses `findNamespace()` for non-global ACLs
- The returned `DefaultACLInfo` array must be freed by the caller
- Uses `selectDumpableDefaultACL()` which may have different criteria than regular `selectDumpableObject()`

## Simplified Source

```c
DefaultACLInfo *
getDefaultACLs(Archive *fout, int *numDefaultACLs)
{
    DumpOptions *dopt = fout->dopt;
    DefaultACLInfo *daclinfo;
    PQExpBuffer query;
    PGresult *res;
    int i, ntups;

    query = createPQExpBuffer();

    // Query default ACLs with special handling for global vs namespace-specific
    appendPQExpBufferStr(query,
        "SELECT oid, tableoid, "
        "defaclrole, "
        "defaclnamespace, "
        "defaclobjtype, "
        "defaclacl, "
        "CASE WHEN defaclnamespace = 0 THEN "
        "acldefault(CASE WHEN defaclobjtype = 'S' "
        "THEN 's'::\"char\" ELSE defaclobjtype END, "
        "defaclrole) ELSE '{}' END AS acldefault "
        "FROM pg_default_acl");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numDefaultACLs = ntups;

    // Allocate array for default ACL info
    daclinfo = (DefaultACLInfo *) pg_malloc(ntups * sizeof(DefaultACLInfo));

    // Get column indices
    int i_oid = PQfnumber(res, "oid");
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_defaclrole = PQfnumber(res, "defaclrole");
    int i_defaclnamespace = PQfnumber(res, "defaclnamespace");
    int i_defaclobjtype = PQfnumber(res, "defaclobjtype");
    int i_defaclacl = PQfnumber(res, "defaclacl");
    int i_acldefault = PQfnumber(res, "acldefault");

    // Process each default ACL
    for (i = 0; i < ntups; i++) {
        Oid nspid = atooid(PQgetvalue(res, i, i_defaclnamespace));

        // Set object type and catalog info
        daclinfo[i].dobj.objType = DO_DEFAULT_ACL;
        daclinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        daclinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&daclinfo[i].dobj);

        // Use object type character as name
        daclinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_defaclobjtype));

        // Set namespace (NULL for global ACLs)
        if (nspid != InvalidOid)
            daclinfo[i].dobj.namespace = findNamespace(nspid);
        else
            daclinfo[i].dobj.namespace = NULL;

        // Set ACL information
        daclinfo[i].dacl.acl = pg_strdup(PQgetvalue(res, i, i_defaclacl));
        daclinfo[i].dacl.acldefault = pg_strdup(PQgetvalue(res, i, i_acldefault));
        daclinfo[i].dacl.privtype = 0;
        daclinfo[i].dacl.initprivs = NULL;

        // Set role and object type
        daclinfo[i].defaclrole = getRoleName(PQgetvalue(res, i, i_defaclrole));
        daclinfo[i].defaclobjtype = *(PQgetvalue(res, i, i_defaclobjtype));

        // Default ACLs are ACL objects by definition
        daclinfo[i].dobj.components |= DUMP_COMPONENT_ACL;

        // Determine if default ACL should be dumped
        selectDumpableDefaultACL(&(daclinfo[i]), dopt);
    }

    // Cleanup and return
    PQclear(res);
    destroyPQExpBuffer(query);
    return daclinfo;
}
```