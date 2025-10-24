# dumpTSTemplate

## Location
[src/bin/pg_dump/pg_dump.c:14731-14788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L14731-L14788)

## Overview
Writes out a single text search template definition to the PostgreSQL dump output, generating the necessary CREATE TEXT SEARCH TEMPLATE statement with lexize and optional init functions.

## Definition

```c
static void
dumpTSTemplate(Archive *fout, const TSTemplateInfo *tmplinfo)
```
## Detailed Description
The  function is responsible for dumping text search template objects during a pg_dump operation. It generates the CREATE TEXT SEARCH TEMPLATE statement with the required LEXIZE function and optional INIT function. Text search templates define the interface between dictionaries and their underlying implementation functions, serving as a blueprint for dictionary creation.

The function constructs both creation and drop statements, handles binary upgrade scenarios, and dumps associated comments. The INIT function is optional and only included if tmplinfo->tmplinit is not InvalidOid.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump configuration and output methods
- `*tmplinfo`: TSTemplateInfo structure containing template metadata including init and lexize function OIDs
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [convertTSFunction](../c/convertTSFunction.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - free
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (via switch statement for DO_TSTEMPLATE objects)

## Notes and Other Information
- Only executes during schema dumps (skipped when dopt->dataOnly is true)
- Handles optional INIT function (only included if tmplinfo->tmplinit != InvalidOid)
- LEXIZE function is always required and included in the template definition
- Supports binary upgrade mode with appropriate extension member handling
- Generates both CREATE and DROP statements for complete dump/restore capability
- Part of PostgreSQL's text search infrastructure dumping functionality
- Templates serve as blueprints for creating dictionaries and define the function interface
- Uses qualified names to handle schema-qualified template names properly
- Comments are dumped without owner information (empty string passed to dumpComment)

## Simplified Source

```c
static void
dumpTSTemplate(Archive *fout, const TSTemplateInfo *tmplinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q;
    PQExpBuffer delq;
    char       *qtmplname;

    // Skip in data-only dump mode
    if (dopt->dataOnly)
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();

    qtmplname = pg_strdup(fmtId(tmplinfo->dobj.name));

    // Build CREATE TEXT SEARCH TEMPLATE statement
    appendPQExpBuffer(q, "CREATE TEXT SEARCH TEMPLATE %s (\n",
                      fmtQualifiedDumpable(tmplinfo));

    // Add optional INIT function
    if (tmplinfo->tmplinit != InvalidOid)
        appendPQExpBuffer(q, "    INIT = %s,\n",
                          convertTSFunction(fout, tmplinfo->tmplinit));

    // Add required LEXIZE function
    appendPQExpBuffer(q, "    LEXIZE = %s );\n",
                      convertTSFunction(fout, tmplinfo->tmpllexize));

    // Build DROP statement
    appendPQExpBuffer(delq, "DROP TEXT SEARCH TEMPLATE %s;\n",
                      fmtQualifiedDumpable(tmplinfo));

    // Handle binary upgrade mode
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &tmplinfo->dobj,
                                        "TEXT SEARCH TEMPLATE", qtmplname,
                                        tmplinfo->dobj.namespace->dobj.name);

    // Archive the template definition
    if (tmplinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, tmplinfo->dobj.catId, tmplinfo->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = tmplinfo->dobj.name,
                                  .namespace = tmplinfo->dobj.namespace->dobj.name,
                                  .description = "TEXT SEARCH TEMPLATE",
                                  .section = SECTION_PRE_DATA,
                                  .createStmt = q->data,
                                  .dropStmt = delq->data));

    // Dump template comments
    if (tmplinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "TEXT SEARCH TEMPLATE", qtmplname,
                    tmplinfo->dobj.namespace->dobj.name, "",
                    tmplinfo->dobj.catId, 0, tmplinfo->dobj.dumpId);

    // Cleanup
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    free(qtmplname);
}
```