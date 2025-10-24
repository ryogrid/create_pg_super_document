# dumpTable

## Location
[src/bin/pg_dump/pg_dump.c:15717-15856](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15717-L15856)

## Overview
Writes the schema declarations (not data) of a user-defined table, including table definition, ACLs, and column-level ACLs to the archive output.

## Definition

```c
static void
dumpTable(Archive *fout, const TableInfo *tbinfo)
```
## Detailed Description
This function handles the complete schema dumping process for tables and sequences, coordinating the output of table definitions, access control lists (ACLs), and column-level permissions. It serves as the main dispatcher that determines what components of a table need to be dumped based on the dump configuration and delegates to appropriate specialized functions.

The function operates in several phases: first, it dumps the table definition itself (either as a sequence or regular table schema); then it handles table-level ACLs if they exist; and finally it processes column-level ACLs using prepared statements for efficiency. Column ACLs require special handling because they can exist on system columns and need to be retrieved separately from the main table attributes query.

For column ACLs, the function uses prepared statements that adapt to different PostgreSQL versions, handling the evolution of the privilege system including the addition of initial privileges tracking in version 9.6. Each column ACL is dumped as a dependent object that relies on the table-level ACL.

## Parameters / Member Variables
- `*fout`: Archive structure for output, containing database connection and dump options
- `*tbinfo`: TableInfo structure containing complete metadata about the table including columns, ACLs, and relationships
## Dependencies
- Functions called/Symbols referenced:
  - [dumpSequence](dumpSequence.md): Dumps sequence definition for RELKIND_SEQUENCE tables
  - [dumpTableSchema](dumpTableSchema.md): Dumps regular table schema definition
  - [pg_strdup](../p/pg_strdup.md): Duplicates strings for memory management
  - [fmtId](../f/fmtId.md): Formats identifiers with proper SQL quoting
  - [dumpACL](dumpACL.md): Dumps access control lists for objects
  - [createPQExpBuffer](../c/createPQExpBuffer.md): Creates buffer for SQL query construction
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md): Adds SQL text to query buffer
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md): Executes prepared statement creation
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md): Formats parameterized queries
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md): Executes column ACL retrieval query
  - [PQgetvalue](../P/PQgetvalue.md): Extracts values from query results
  - [PQclear](../P/PQclear.md): Frees query result memory
  - [destroyPQExpBuffer](destroyPQExpBuffer.md): Cleans up query buffers
- Called from:
  - [dumpDumpableObject](dumpDumpableObject.md): Called as part of general object dumping dispatcher
  - fmtQualifiedDumpable: Referenced for qualified name formatting

## Notes and Other Information
- Skips execution entirely if dataOnly dump option is set
- Handles both tables and sequences through appropriate delegation
- Uses prepared statements for column ACL queries to improve performance when processing multiple tables
- Column ACL queries adapt to PostgreSQL version differences, particularly around initial privileges introduced in 9.6
- Column ACLs depend on table-level ACLs to ensure correct restoration order
- Hard-codes column default ACL as empty ('{}') to avoid complex owner name resolution
- Memory management includes proper cleanup of duplicated strings
- Part of the schema dumping infrastructure, focusing solely on structure and permissions, not data
- Supports parallel dumping by properly establishing dependencies between related ACL objects

## Simplified Source

```c
static void
dumpTable(Archive *fout, const TableInfo *tbinfo)
{
    DumpOptions *dopt = fout->dopt;
    DumpId tableAclDumpId = InvalidDumpId;
    char *namecopy;

    // Skip if data-only dump
    if (dopt->dataOnly)
        return;

    // Dump table definition
    if (tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
    {
        if (tbinfo->relkind == RELKIND_SEQUENCE)
            dumpSequence(fout, tbinfo);
        else
            dumpTableSchema(fout, tbinfo);
    }

    // Handle table-level ACL
    namecopy = pg_strdup(fmtId(tbinfo->dobj.name));
    if (tbinfo->dobj.dump & DUMP_COMPONENT_ACL)
    {
        const char *objtype =
            (tbinfo->relkind == RELKIND_SEQUENCE) ? "SEQUENCE" : "TABLE";

        tableAclDumpId = dumpACL(fout, tbinfo->dobj.dumpId, InvalidDumpId,
                                objtype, namecopy, NULL,
                                tbinfo->dobj.namespace->dobj.name,
                                NULL, tbinfo->rolname, &tbinfo->dacl);
    }

    // Handle column ACLs if present
    if ((tbinfo->dobj.dump & DUMP_COMPONENT_ACL) && tbinfo->hascolumnACLs)
    {
        PQExpBuffer query = createPQExpBuffer();
        PGresult *res;

        // Set up prepared statement for column ACLs
        if (!fout->is_prepared[PREPQUERY_GETCOLUMNACLS])
        {
            // Build version-specific query for column ACLs
            appendPQExpBufferStr(query, "PREPARE getColumnACLs(pg_catalog.oid) AS\n");

            if (fout->remoteVersion >= 90600)
            {
                // Query with initial privileges support (9.6+)
                appendPQExpBufferStr(query,
                    "SELECT at.attname, at.attacl, '{}' AS acldefault, "
                    "pip.privtype, pip.initprivs FROM pg_catalog.pg_attribute at "
                    "LEFT JOIN pg_catalog.pg_init_privs pip ON "
                    "(at.attrelid = pip.objoid AND pip.classoid = 'pg_catalog.pg_class'::pg_catalog.regclass "
                    "AND at.attnum = pip.objsubid) "
                    "WHERE at.attrelid = $1 AND NOT at.attisdropped "
                    "AND (at.attacl IS NOT NULL OR pip.initprivs IS NOT NULL) "
                    "ORDER BY at.attnum");
            }
            else
            {
                // Legacy query for older versions
                appendPQExpBufferStr(query,
                    "SELECT attname, attacl, '{}' AS acldefault, "
                    "NULL AS privtype, NULL AS initprivs "
                    "FROM pg_catalog.pg_attribute "
                    "WHERE attrelid = $1 AND NOT attisdropped "
                    "AND attacl IS NOT NULL ORDER BY attnum");
            }

            ExecuteSqlStatement(fout, query->data);
            fout->is_prepared[PREPQUERY_GETCOLUMNACLS] = true;
        }

        // Execute query for this table's column ACLs
        printfPQExpBuffer(query, "EXECUTE getColumnACLs('%u')",
                         tbinfo->dobj.catId.oid);
        res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

        // Process each column ACL
        for (int i = 0; i < PQntuples(res); i++)
        {
            char *attname = PQgetvalue(res, i, 0);
            char *attacl = PQgetvalue(res, i, 1);
            char *acldefault = PQgetvalue(res, i, 2);
            char privtype = *(PQgetvalue(res, i, 3));
            char *initprivs = PQgetvalue(res, i, 4);
            DumpableAcl coldacl;
            char *attnamecopy;

            // Set up column ACL structure
            coldacl.acl = attacl;
            coldacl.acldefault = acldefault;
            coldacl.privtype = privtype;
            coldacl.initprivs = initprivs;
            attnamecopy = pg_strdup(fmtId(attname));

            // Dump column ACL with dependency on table ACL
            dumpACL(fout, tbinfo->dobj.dumpId, tableAclDumpId,
                   "TABLE", namecopy, attnamecopy,
                   tbinfo->dobj.namespace->dobj.name,
                   NULL, tbinfo->rolname, &coldacl);

            free(attnamecopy);
        }

        PQclear(res);
        destroyPQExpBuffer(query);
    }

    free(namecopy);
}
```