# dumpNamespace

## Location
[src/bin/pg_dump/pg_dump.c:10715-10791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L10715-L10791)

## Overview
Generates SQL commands to recreate a user-defined namespace (schema) in pg_dump output, including CREATE/DROP statements and associated metadata.

## Definition

```c
static void
dumpNamespace(Archive *fout, const NamespaceInfo *nspinfo)
```
## Detailed Description
The  function creates the necessary SQL statements to recreate a PostgreSQL schema during database restore. It handles both user-created schemas and system schemas that are created by initdb. The function generates CREATE SCHEMA and DROP SCHEMA statements, but provides special handling for schemas like 'public' that are created during database initialization.

The function also handles binary upgrade scenarios and dumps associated metadata including comments, security labels, and access control lists (ACLs). For schemas not created by the user (like 'public'), it generates comments indicating that the schema is not being created since initdb handles it.

## Parameters / Member Variables
- `*fout`: Archive structure representing the dump destination and containing connection/output information
- `*nspinfo`: Pointer to NamespaceInfo structure containing schema metadata including name, owner, creation flag, and dump components
## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpCommentExtended](dumpCommentExtended.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (in pg_dump.c:10538)

## Notes and Other Information
- Skips processing entirely in data-only dump mode ()
- Special handling for the 'public' schema with predefined comment "standard public schema"
- Supports binary upgrade mode with extension membership tracking
- Conditionally dumps definition, comments, security labels, and ACLs based on dump component flags
- Uses formatted identifiers to handle schemas with special characters or reserved words
- Memory management includes proper cleanup of allocated PQExpBuffer and string resources

## Simplified Source

```c
static void dumpNamespace(Archive *fout, const NamespaceInfo *nspinfo) {
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q;
    PQExpBuffer delq;
    char *qnspname;

    // Skip in data-only mode
    if (dopt->dataOnly)
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();

    qnspname = pg_strdup(fmtId(nspinfo->dobj.name));

    // Generate CREATE/DROP statements
    if (nspinfo->create) {
        appendPQExpBuffer(delq, "DROP SCHEMA %s;\n", qnspname);
        appendPQExpBuffer(q, "CREATE SCHEMA %s;\n", qnspname);
    } else {
        // Schema created by initdb - add comments explaining why not creating
        appendPQExpBufferStr(delq,
                             "-- *not* dropping schema, since initdb creates it\n");
        appendPQExpBufferStr(q,
                             "-- *not* creating schema, since initdb creates it\n");
    }

    // Handle binary upgrade mode
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &nspinfo->dobj,
                                        "SCHEMA", qnspname, NULL);

    // Create archive entry for schema definition
    if (nspinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, nspinfo->dobj.catId, nspinfo->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = nspinfo->dobj.name,
                                  .owner = nspinfo->rolname,
                                  .description = "SCHEMA",
                                  .section = SECTION_PRE_DATA,
                                  .createStmt = q->data,
                                  .dropStmt = delq->data));

    // Dump schema comments
    if (nspinfo->dobj.dump & DUMP_COMPONENT_COMMENT) {
        const char *initdb_comment = NULL;

        if (!nspinfo->create && strcmp(qnspname, "public") == 0)
            initdb_comment = "standard public schema";
        dumpCommentExtended(fout, "SCHEMA", qnspname,
                            NULL, nspinfo->rolname,
                            nspinfo->dobj.catId, 0, nspinfo->dobj.dumpId,
                            initdb_comment);
    }

    // Dump security labels
    if (nspinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
        dumpSecLabel(fout, "SCHEMA", qnspname,
                     NULL, nspinfo->rolname,
                     nspinfo->dobj.catId, 0, nspinfo->dobj.dumpId);

    // Dump ACLs
    if (nspinfo->dobj.dump & DUMP_COMPONENT_ACL)
        dumpACL(fout, nspinfo->dobj.dumpId, InvalidDumpId, "SCHEMA",
                qnspname, NULL, NULL,
                NULL, nspinfo->rolname, &nspinfo->dacl);

    free(qnspname);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
}
```