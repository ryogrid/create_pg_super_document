# dumpTSParser

## Location
[src/bin/pg_dump/pg_dump.c:14587-14650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L14587-L14650)

## Overview
Writes out a single text search parser definition to the PostgreSQL dump output, generating the necessary CREATE TEXT SEARCH PARSER statement.

## Definition

```c
static void
dumpTSParser(Archive *fout, const TSParserInfo *prsinfo)
```
## Detailed Description
The  function is responsible for dumping text search parser objects during a pg_dump operation. It generates the CREATE TEXT SEARCH PARSER statement with all required function references (START, GETTOKEN, END, optional HEADLINE, and LEXTYPES) and handles proper formatting, binary upgrade compatibility, and comment dumping.

The function skips execution during data-only dumps and constructs both creation and drop statements for the parser. It also handles binary upgrade scenarios and dumps associated comments if requested.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump configuration and output methods
- `*prsinfo`: TSParserInfo structure containing all parser metadata including function OIDs and parser properties
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [convertTSFunction](../c/convertTSFunction.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - free
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (via switch statement for DO_TSPARSER objects)

## Notes and Other Information
- Only executes during schema dumps (skipped when dopt->dataOnly is true)
- Handles optional HEADLINE function (only included if prsinfo->prsheadline != InvalidOid)
- Supports binary upgrade mode with appropriate extension member handling
- Generates both CREATE and DROP statements for complete dump/restore capability
- Part of PostgreSQL's text search infrastructure dumping functionality
- Uses qualified names to handle schema-qualified parser names properly

## Simplified Source

```c
static void
dumpTSParser(Archive *fout, const TSParserInfo *prsinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q, delq;
    char *qprsname;

    // Skip in data-only mode
    if (dopt->dataOnly)
        return;

    // Initialize buffers and format parser name
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    qprsname = pg_strdup(fmtId(prsinfo->dobj.name));

    // Build CREATE TEXT SEARCH PARSER statement
    appendPQExpBuffer(q, "CREATE TEXT SEARCH PARSER %s (\n",
                      fmtQualifiedDumpable(prsinfo));

    // Add required functions
    appendPQExpBuffer(q, "    START = %s,\n",
                      convertTSFunction(fout, prsinfo->prsstart));
    appendPQExpBuffer(q, "    GETTOKEN = %s,\n",
                      convertTSFunction(fout, prsinfo->prstoken));
    appendPQExpBuffer(q, "    END = %s,\n",
                      convertTSFunction(fout, prsinfo->prsend));

    // Add optional HEADLINE function
    if (prsinfo->prsheadline != InvalidOid)
        appendPQExpBuffer(q, "    HEADLINE = %s,\n",
                         convertTSFunction(fout, prsinfo->prsheadline));

    // Add required LEXTYPES function
    appendPQExpBuffer(q, "    LEXTYPES = %s );\n",
                      convertTSFunction(fout, prsinfo->prslextype));

    // Build DROP statement
    appendPQExpBuffer(delq, "DROP TEXT SEARCH PARSER %s;\n",
                      fmtQualifiedDumpable(prsinfo));

    // Handle binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &prsinfo->dobj,
                                       "TEXT SEARCH PARSER", qprsname,
                                       prsinfo->dobj.namespace->dobj.name);

    // Register with archive
    if (prsinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, prsinfo->dobj.catId, prsinfo->dobj.dumpId,
                    ARCHIVE_OPTS(.tag = prsinfo->dobj.name,
                                .namespace = prsinfo->dobj.namespace->dobj.name,
                                .description = "TEXT SEARCH PARSER",
                                .section = SECTION_PRE_DATA,
                                .createStmt = q->data,
                                .dropStmt = delq->data));

    // Dump comments
    if (prsinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "TEXT SEARCH PARSER", qprsname,
                   prsinfo->dobj.namespace->dobj.name, "",
                   prsinfo->dobj.catId, 0, prsinfo->dobj.dumpId);

    // Cleanup
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    free(qprsname);
}
```