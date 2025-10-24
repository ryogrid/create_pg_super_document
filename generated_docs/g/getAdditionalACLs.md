# getAdditionalACLs

## Location
[src/bin/pg_dump/pg_dump.c:10017-10145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L10017-L10145)

## Overview
Collects additional ACL-related information for database objects that was not captured during initial object collection, including per-column ACLs and initial privileges from pg_init_privs catalog.

## Definition

```c
static void
getAdditionalACLs(Archive *fout)
```
## Detailed Description
This function performs post-processing to gather ACL information that requires all DumpableObjects to be created first. It operates in two main phases:

1. **Column ACL Detection**: Queries pg_attribute to find tables with column-level ACLs and marks the corresponding TableInfo objects with the DUMP_COMPONENT_ACL flag and hascolumnACLs flag.

2. **Initial Privileges Collection**: For PostgreSQL 9.6+, reads the pg_init_privs catalog to collect initial privilege information for various database objects and stores this data in the corresponding DumpableObjectWithAcl structures.

The function ensures that objects with column-level privileges or initial privileges are properly flagged for ACL dumping during the backup process.

## Parameters / Member Variables
- `*fout`: Archive context containing connection information and version details for the database being dumped
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - atooid
  - [findTableByOid](../f/findTableByOid.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [findObjectByCatalogId](../f/findObjectByCatalogId.md)
  - pg_log_warning
  - [pstrdup](../p/pstrdup.md)
- Called from (representative examples):
  - [main](../m/main.md) (pg_dump main function)

## Notes and Other Information
- Only processes pg_init_privs data for PostgreSQL 9.6 and later versions
- Skips pg_init_privs entries for the "public" schema as explained in getNamespaces()
- Supports ACL collection for specific object types: namespaces, types, functions, aggregates, tables, procedural languages, foreign data wrappers, and foreign servers
- Issues warnings for unsupported pg_init_privs entries
- Does not store actual column ACL data but only marks tables as having column ACLs

## Simplified Source

```c
static void getAdditionalACLs(Archive *fout) {
    PQExpBuffer query = createPQExpBuffer();
    PGresult *res;
    int ntups, i;

    // Find tables with column-level ACLs
    appendPQExpBufferStr(query,
                         "SELECT DISTINCT attrelid FROM pg_attribute "
                         "WHERE attacl IS NOT NULL");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Mark tables that have column ACLs
    for (i = 0; i < ntups; i++) {
        Oid relid = atooid(PQgetvalue(res, i, 0));
        TableInfo *tblinfo = findTableByOid(relid);

        if (tblinfo) {
            tblinfo->dobj.components |= DUMP_COMPONENT_ACL;
            tblinfo->hascolumnACLs = true;
        }
    }
    PQclear(res);

    // Collect initial privileges for PostgreSQL 9.6+
    if (fout->remoteVersion >= 90600) {
        printfPQExpBuffer(query,
                          "SELECT objoid, classoid, objsubid, privtype, initprivs "
                          "FROM pg_init_privs");

        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
        ntups = PQntuples(res);

        for (i = 0; i < ntups; i++) {
            Oid objoid = atooid(PQgetvalue(res, i, 0));
            Oid classoid = atooid(PQgetvalue(res, i, 1));
            int objsubid = atoi(PQgetvalue(res, i, 2));
            char privtype = *(PQgetvalue(res, i, 3));
            char *initprivs = PQgetvalue(res, i, 4);

            CatalogId objId = {classoid, objoid};
            DumpableObject *dobj = findObjectByCatalogId(objId);

            if (dobj) {
                // Handle column-level privileges
                if (objsubid != 0) {
                    if (dobj->objType == DO_TABLE) {
                        dobj->components |= DUMP_COMPONENT_ACL;
                        ((TableInfo *) dobj)->hascolumnACLs = true;
                    }
                    continue;
                }

                // Skip public schema
                if (dobj->objType == DO_NAMESPACE &&
                    strcmp(dobj->name, "public") == 0)
                    continue;

                // Store initial privileges for supported object types
                if (dobj->objType == DO_NAMESPACE || dobj->objType == DO_TYPE ||
                    dobj->objType == DO_FUNC || dobj->objType == DO_AGG ||
                    dobj->objType == DO_TABLE || dobj->objType == DO_PROCLANG ||
                    dobj->objType == DO_FDW || dobj->objType == DO_FOREIGN_SERVER) {

                    DumpableObjectWithAcl *daobj = (DumpableObjectWithAcl *) dobj;
                    daobj->dacl.privtype = privtype;
                    daobj->dacl.initprivs = pstrdup(initprivs);
                }
            }
        }
        PQclear(res);
    }

    destroyPQExpBuffer(query);
}
```