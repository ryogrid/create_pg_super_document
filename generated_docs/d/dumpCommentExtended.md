# dumpCommentExtended

## Location
[src/bin/pg_dump/pg_dump.c:10146-10245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L10146-L10245)

## Overview
Dumps comments associated with database objects by searching for matching pg_description entries and generating COMMENT ON statements in the dump output.

## Definition

```c
static void
dumpCommentExtended(Archive *fout, const char *type,
					const char *name, const char *namespace,
					const char *owner, CatalogId catalogId,
					int subid, DumpId dumpId,
					const char *initdb_comment)
```
## Detailed Description
This function is responsible for dumping comments for database objects during a pg_dump operation. It searches the pg_description catalog for comments matching the specified catalogId and subid, then generates appropriate COMMENT ON SQL statements. The function handles special cases such as:

- Large Object comments (treated as data rather than schema)
- initdb-created comments (skipped to avoid complications for non-superuser dumps)
- Proper dependency tracking in the dump file
- Section placement (marked as SECTION_NONE to belong with parent object)

The function respects dump options like --no-comments, --data-only, and --schema-only to determine whether comments should be included in the output.

## Parameters / Member Variables
- `*fout`: Archive context for the dump operation
- `*type`: Object type string (e.g., "TABLE", "FUNCTION", "TRIGGER name ON")
- `*name`: Object name ready for printing (without schema decoration)
- `*namespace`: Schema namespace of the object for labeling
- `*owner`: Owner of the object for labeling
- `catalogId`: Catalog identifier (tableoid and oid) for pg_description lookup
- `subid`: Sub-object identifier for pg_description lookup (0 for main object)
- `dumpId`: Dump ID for dependency tracking in the output
- `*initdb_comment`: Expected comment text created by initdb (NULL if none)
## Dependencies
- Functions called/Symbols referenced:
  - [findComments](../f/findComments.md)
  - [fmtId](../f/fmtId.md)
  - appendStringLiteralAH
  - [createDumpId](../c/createDumpId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - ARCHIVE_OPTS
- Called from (representative examples):
  - [dumpComment](dumpComment.md)
  - [dumpNamespace](dumpNamespace.md)

## Notes and Other Information
- Comments are marked as SECTION_NONE so they appear in the same section as their parent object
- The routine should be called immediately after calling ArchiveEntry() for the associated object
- Large Object comments are treated as data, not schema, for dump filtering purposes
- Special handling for initdb comments prevents dumping system-provided comments that would complicate non-superuser usage
- The function handles cases where initdb comments have been removed by the DBA

## Simplified Source

```c
static void dumpCommentExtended(Archive *fout, const char *type,
                                const char *name, const char *namespace,
                                const char *owner, CatalogId catalogId,
                                int subid, DumpId dumpId,
                                const char *initdb_comment) {
    DumpOptions *dopt = fout->dopt;
    CommentItem *comments;
    int ncomments;

    // Skip if comments are disabled
    if (dopt->no_comments)
        return;

    // Handle data-only vs schema-only modes
    if (strcmp(type, "LARGE OBJECT") != 0) {
        if (dopt->dataOnly)
            return;
    } else {
        if (dopt->schemaOnly && !dopt->binary_upgrade)
            return;
    }

    // Find comments for this object
    ncomments = findComments(catalogId.tableoid, catalogId.oid, &comments);

    // Search for comment matching the subid
    while (ncomments > 0) {
        if (comments->objsubid == subid)
            break;
        comments++;
        ncomments--;
    }

    // Handle initdb comment special cases
    if (initdb_comment != NULL) {
        static CommentItem empty_comment = {.descr = ""};

        if (ncomments == 0) {
            comments = &empty_comment;
            ncomments = 1;
        } else if (strcmp(comments->descr, initdb_comment) == 0) {
            ncomments = 0;
        }
    }

    // Generate COMMENT ON statement if comment exists
    if (ncomments > 0) {
        PQExpBuffer query = createPQExpBuffer();
        PQExpBuffer tag = createPQExpBuffer();

        appendPQExpBuffer(query, "COMMENT ON %s ", type);
        if (namespace && *namespace)
            appendPQExpBuffer(query, "%s.", fmtId(namespace));
        appendPQExpBuffer(query, "%s IS ", name);
        appendStringLiteralAH(query, comments->descr, fout);
        appendPQExpBufferStr(query, ";\n");

        appendPQExpBuffer(tag, "%s %s", type, name);

        ArchiveEntry(fout, nilCatalogId, createDumpId(),
                     ARCHIVE_OPTS(.tag = tag->data,
                                  .namespace = namespace,
                                  .owner = owner,
                                  .description = "COMMENT",
                                  .section = SECTION_NONE,
                                  .createStmt = query->data,
                                  .deps = &dumpId,
                                  .nDeps = 1));

        destroyPQExpBuffer(query);
        destroyPQExpBuffer(tag);
    }
}
```