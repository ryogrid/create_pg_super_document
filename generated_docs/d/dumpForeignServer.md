# dumpForeignServer

## Location
[src/bin/pg_dump/pg_dump.c:14979-15078](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L14979-L15078)

## Overview
Generates and outputs the SQL statements needed to recreate a foreign server definition during database dump operations in pg_dump.

## Definition

```c
static void
dumpForeignServer(Archive *fout, const ForeignServerInfo *srvinfo)
```
## Detailed Description
The  function is responsible for creating the SQL DDL statements necessary to recreate a foreign server object when restoring a database dump. It handles the complete lifecycle of foreign server dumping including:

1. **CREATE SERVER statement generation** - Constructs the complete CREATE SERVER command with all specified options (type, version, foreign data wrapper, and server options)
2. **DROP SERVER statement generation** - Creates the corresponding DROP statement for clean restoration
3. **Binary upgrade support** - Handles special cases for binary upgrades through extension member processing
4. **Associated metadata dumping** - Automatically triggers dumping of related objects including comments, ACLs, and user mappings

The function operates as part of pg_dump's comprehensive database export process and ensures that foreign server definitions are properly preserved and can be restored with all their associated properties and permissions.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump configuration and output methods for writing the generated SQL statements
- `*srvinfo`: ForeignServerInfo structure containing all the metadata about the foreign server to be dumped, including name, type, version, foreign data wrapper reference, options, owner, and access control information
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [pg_strdup](../p/pg_strdup.md) 
  - [fmtId](../f/fmtId.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - appendStringLiteralAH
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpACL](dumpACL.md)
  - [dumpUserMappings](dumpUserMappings.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- The function respects the data-only dump option and returns early without generating any output if only data is being dumped
- Foreign server options are formatted with proper indentation and newlines for readability in the output SQL
- The function automatically handles dumping of associated objects (comments, ACLs, user mappings) based on the dump component flags
- Binary upgrade scenarios receive special handling to maintain system catalog consistency
- Memory management is properly handled with PQExpBuffer creation and destruction

## Simplified Source

```c
static void
dumpForeignServer(Archive *fout, const ForeignServerInfo *srvinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q, delq, query;
    PGresult *res;
    char *qsrvname;
    char *fdwname;

    // Skip if data-only dump
    if (dopt->dataOnly)
        return;

    // Initialize buffers and format server name
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    query = createPQExpBuffer();
    qsrvname = pg_strdup(fmtId(srvinfo->dobj.name));

    // Look up the foreign-data wrapper name
    appendPQExpBuffer(query, "SELECT fdwname "
                      "FROM pg_foreign_data_wrapper w "
                      "WHERE w.oid = '%u'", srvinfo->srvfdw);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);
    fdwname = PQgetvalue(res, 0, 0);

    // Build CREATE SERVER statement
    appendPQExpBuffer(q, "CREATE SERVER %s", qsrvname);

    // Add optional TYPE clause
    if (srvinfo->srvtype && strlen(srvinfo->srvtype) > 0) {
        appendPQExpBufferStr(q, " TYPE ");
        appendStringLiteralAH(q, srvinfo->srvtype, fout);
    }

    // Add optional VERSION clause
    if (srvinfo->srvversion && strlen(srvinfo->srvversion) > 0) {
        appendPQExpBufferStr(q, " VERSION ");
        appendStringLiteralAH(q, srvinfo->srvversion, fout);
    }

    // Add foreign data wrapper reference
    appendPQExpBufferStr(q, " FOREIGN DATA WRAPPER ");
    appendPQExpBufferStr(q, fmtId(fdwname));

    // Add server options if present
    if (srvinfo->srvoptions && strlen(srvinfo->srvoptions) > 0)
        appendPQExpBuffer(q, " OPTIONS (\n    %s\n)", srvinfo->srvoptions);

    appendPQExpBufferStr(q, ";\n");

    // Generate DROP statement
    appendPQExpBuffer(delq, "DROP SERVER %s;\n", qsrvname);

    // Handle binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &srvinfo->dobj,
                                        "SERVER", qsrvname, NULL);

    // Create archive entry
    if (srvinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, srvinfo->dobj.catId, srvinfo->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = srvinfo->dobj.name,
                                  .owner = srvinfo->rolname,
                                  .description = "SERVER",
                                  .createStmt = q->data,
                                  .dropStmt = delq->data));

    // Dump associated objects
    if (srvinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "SERVER", qsrvname, NULL, srvinfo->rolname,
                    srvinfo->dobj.catId, 0, srvinfo->dobj.dumpId);

    if (srvinfo->dobj.dump & DUMP_COMPONENT_ACL)
        dumpACL(fout, srvinfo->dobj.dumpId, InvalidDumpId,
                "FOREIGN SERVER", qsrvname, NULL, NULL,
                NULL, srvinfo->rolname, &srvinfo->dacl);

    if (srvinfo->dobj.dump & DUMP_COMPONENT_USERMAP)
        dumpUserMappings(fout, srvinfo->dobj.name, NULL,
                         srvinfo->rolname, srvinfo->dobj.catId, srvinfo->dobj.dumpId);

    // Cleanup
    PQclear(res);
    free(qsrvname);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(query);
}
```