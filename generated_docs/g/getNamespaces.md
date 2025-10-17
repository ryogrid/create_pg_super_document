# getNamespaces

## Location
[src/bin/pg_dump/pg_dump.c:5636-5753](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5636-L5753)

## Overview
Reads all namespaces (schemas) from the PostgreSQL system catalogs and returns them as an array of NamespaceInfo structures for pg_dump processing.

## Definition

```c
NamespaceInfo *
getNamespaces(Archive *fout, int *numNamespaces)
```
## Detailed Description
This function is a core part of pg_dump's metadata collection process. It queries the pg_namespace system catalog to retrieve information about all namespaces (schemas) in the database, including system schemas. Each namespace is converted into a NamespaceInfo structure containing the necessary metadata for dumping.

The function performs several important tasks:
1. Executes a SQL query to fetch namespace metadata including OID, name, owner, and ACL information
2. Creates NamespaceInfo structures for each namespace with proper dump object initialization
3. Determines which namespaces should be dumped based on dump configuration
4. Handles special ACL processing for the 'public' schema to ensure consistency across PostgreSQL versions
5. Sets up component flags to indicate whether ACL information should be dumped

## Parameters / Member Variables
- `*fout`: Archive structure containing dump configuration and output methods
- `*numNamespaces`: Output parameter that receives the total number of namespaces found
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [getRoleName](getRoleName.md)
  - [selectDumpableNamespace](../s/selectDumpableNamespace.md)
  - [quoteAclUserName](../q/quoteAclUserName.md)
  - [appendPGArray](../a/appendPGArray.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Fetches ALL namespaces including system ones to ensure proper object linking
- Special handling for 'public' schema ACLs to maintain consistency across PostgreSQL versions
- Uses predetermined default ACLs for public schema rather than pg_init_privs entries
- Supports dump/reload of public schema ownership changes
- Sets DUMP_COMPONENT_ACL flag when namespaces have non-null ACL information
- Memory allocation uses pg_malloc for the NamespaceInfo array
- Returns allocated array that must be freed by caller

## Simplified Source

```c
NamespaceInfo *getNamespaces(Archive *fout, int *numNamespaces)
{
    PGresult *res;
    int ntups, i;
    PQExpBuffer query;
    NamespaceInfo *nsinfo;
    int i_tableoid, i_oid, i_nspname, i_nspowner, i_nspacl, i_acldefault;

    query = createPQExpBuffer();

    // Query all namespaces including system ones
    appendPQExpBufferStr(query,
                         "SELECT n.tableoid, n.oid, n.nspname, n.nspowner, "
                         "n.nspacl, acldefault('n', n.nspowner) AS acldefault "
                         "FROM pg_namespace n");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Allocate array for namespace info
    nsinfo = (NamespaceInfo *) pg_malloc(ntups * sizeof(NamespaceInfo));

    // Get column indices
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_nspname = PQfnumber(res, "nspname");
    i_nspowner = PQfnumber(res, "nspowner");
    i_nspacl = PQfnumber(res, "nspacl");
    i_acldefault = PQfnumber(res, "acldefault");

    // Process each namespace
    for (i = 0; i < ntups; i++) {
        const char *nspowner;

        // Initialize dump object
        nsinfo[i].dobj.objType = DO_NAMESPACE;
        nsinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        nsinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&nsinfo[i].dobj);
        nsinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_nspname));

        // Set ACL information
        nsinfo[i].dacl.acl = pg_strdup(PQgetvalue(res, i, i_nspacl));
        nsinfo[i].dacl.acldefault = pg_strdup(PQgetvalue(res, i, i_acldefault));
        nsinfo[i].dacl.privtype = 0;
        nsinfo[i].dacl.initprivs = NULL;

        // Set owner information
        nspowner = PQgetvalue(res, i, i_nspowner);
        nsinfo[i].nspowner = atooid(nspowner);
        nsinfo[i].rolname = getRoleName(nspowner);

        // Determine if this namespace should be dumped
        selectDumpableNamespace(&nsinfo[i], fout);

        // Mark ACL component if present
        if (!PQgetisnull(res, i, i_nspacl))
            nsinfo[i].dobj.components |= DUMP_COMPONENT_ACL;

        // Special handling for 'public' schema ACL
        if (strcmp(nsinfo[i].dobj.name, "public") == 0) {
            PQExpBuffer aclarray = createPQExpBuffer();
            PQExpBuffer aclitem = createPQExpBuffer();

            // Create standard v15+ ACL: {owner=UC/owner,=U/owner}
            appendPQExpBufferChar(aclarray, '{');
            quoteAclUserName(aclitem, nsinfo[i].rolname);
            appendPQExpBufferStr(aclitem, "=UC/");
            quoteAclUserName(aclitem, nsinfo[i].rolname);
            appendPGArray(aclarray, aclitem->data);
            resetPQExpBuffer(aclitem);
            appendPQExpBufferStr(aclitem, "=U/");
            quoteAclUserName(aclitem, nsinfo[i].rolname);
            appendPGArray(aclarray, aclitem->data);
            appendPQExpBufferChar(aclarray, '}');

            nsinfo[i].dacl.privtype = 'i';
            nsinfo[i].dacl.initprivs = pstrdup(aclarray->data);
            nsinfo[i].dobj.components |= DUMP_COMPONENT_ACL;

            destroyPQExpBuffer(aclarray);
            destroyPQExpBuffer(aclitem);
        }
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    *numNamespaces = ntups;
    return nsinfo;
}
```