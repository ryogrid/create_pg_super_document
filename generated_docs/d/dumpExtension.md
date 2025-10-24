# dumpExtension

## Location
[src/bin/pg_dump/pg_dump.c:10792-10919](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L10792-L10919)

## Overview
Generates SQL commands to recreate a PostgreSQL extension during database restore, with different strategies for regular dumps versus binary upgrade scenarios.

## Definition

```c
static void
dumpExtension(Archive *fout, const ExtensionInfo *extinfo)
```
## Detailed Description
The  function creates SQL statements to restore PostgreSQL extensions. It handles two distinct scenarios: regular dumps where it creates extensions using  allowing for flexible version handling, and binary upgrade mode where it precisely recreates the exact extension state including version, configuration, and dependencies.

In regular mode, the function intentionally omits version specification to use the destination installation's default version. In binary upgrade mode, it creates an empty extension with exact metadata and relies on  to add individual objects. The function also handles extension dependencies and configuration arrays while preserving OID relationships during binary upgrades.

## Parameters / Member Variables
- `*fout`: Archive structure representing the dump destination and containing connection/output information
- `*extinfo`: Pointer to ExtensionInfo structure containing extension metadata including name, namespace, version, configuration, condition, and dependencies
## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - appendStringLiteralAH
  - [findObjectByDumpId](../f/findObjectByDumpId.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (in pg_dump.c:10541)

## Notes and Other Information
- Skips processing entirely in data-only dump mode ()
- Uses  clause in regular mode to allow pre-existing extensions
- Binary upgrade mode calls  function
- Handles extension configuration arrays () and conditions () as-is during binary upgrade
- Processes extension dependencies, particularly other extensions in the dependency chain
- Supports dumping extension comments and security labels based on component flags
- Memory management includes proper cleanup of allocated resources and formatted strings

## Simplified Source

```c
static void
dumpExtension(Archive *fout, const ExtensionInfo *extinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q, delq;
    char *qextname;

    // Skip data-only dumps
    if (dopt->dataOnly)
        return;

    // Initialize buffers and format extension name
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    qextname = pg_strdup(fmtId(extinfo->dobj.name));

    // Create DROP statement
    appendPQExpBuffer(delq, "DROP EXTENSION %s;\n", qextname);

    if (!dopt->binary_upgrade)
    {
        // Regular dump: Create extension with IF NOT EXISTS
        // Uses default version for flexibility
        appendPQExpBuffer(q, "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s;\n",
                         qextname, fmtId(extinfo->namespace));
    }
    else
    {
        // Binary upgrade: Create exact replica
        appendPQExpBuffer(q, "DROP EXTENSION IF EXISTS %s;\n", qextname);

        // Call binary_upgrade_create_empty_extension with exact metadata
        appendPQExpBufferStr(q, "SELECT pg_catalog.binary_upgrade_create_empty_extension(");
        appendStringLiteralAH(q, extinfo->dobj.name, fout);
        appendPQExpBufferStr(q, ", ");
        appendStringLiteralAH(q, extinfo->namespace, fout);
        appendPQExpBufferStr(q, ", ");
        appendPQExpBuffer(q, "%s, ", extinfo->relocatable ? "true" : "false");
        appendStringLiteralAH(q, extinfo->extversion, fout);

        // Add configuration and condition arrays
        appendPQExpBufferStr(q, ", ");
        if (strlen(extinfo->extconfig) > 2)
            appendStringLiteralAH(q, extinfo->extconfig, fout);
        else
            appendPQExpBufferStr(q, "NULL");

        appendPQExpBufferStr(q, ", ");
        if (strlen(extinfo->extcondition) > 2)
            appendStringLiteralAH(q, extinfo->extcondition, fout);
        else
            appendPQExpBufferStr(q, "NULL");

        // Build extension dependencies array
        appendPQExpBufferStr(q, ", ARRAY[");
        int n = 0;
        for (int i = 0; i < extinfo->dobj.nDeps; i++)
        {
            DumpableObject *extobj = findObjectByDumpId(extinfo->dobj.dependencies[i]);
            if (extobj && extobj->objType == DO_EXTENSION)
            {
                if (n++ > 0)
                    appendPQExpBufferChar(q, ',');
                appendStringLiteralAH(q, extobj->name, fout);
            }
        }
        appendPQExpBufferStr(q, "]::pg_catalog.text[]);\n");
    }

    // Archive the extension definition
    if (extinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, extinfo->dobj.catId, extinfo->dobj.dumpId,
                    ARCHIVE_OPTS(.tag = extinfo->dobj.name,
                                .description = "EXTENSION",
                                .section = SECTION_PRE_DATA,
                                .createStmt = q->data,
                                .dropStmt = delq->data));

    // Dump comments and security labels if requested
    if (extinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "EXTENSION", qextname, NULL, "",
                   extinfo->dobj.catId, 0, extinfo->dobj.dumpId);

    if (extinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
        dumpSecLabel(fout, "EXTENSION", qextname, NULL, "",
                    extinfo->dobj.catId, 0, extinfo->dobj.dumpId);

    // Cleanup
    free(qextname);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
}
```