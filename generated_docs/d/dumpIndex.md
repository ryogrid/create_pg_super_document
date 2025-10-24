# dumpIndex

## Location
[src/bin/pg_dump/pg_dump.c:16966-17116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L16966-L17116)

## Overview
Writes out a user-defined index to the dump archive, handling both standalone indexes and constraint-associated indexes with appropriate SQL generation and metadata handling.

## Definition

```c
static void
dumpIndex(Archive *fout, const IndxInfo *indxinfo)
```
## Detailed Description
The  function is responsible for dumping user-defined indexes in pg_dump. It generates the necessary SQL statements to recreate the index, including:

1. **Index Creation**: Uses the stored index definition to create the basic CREATE INDEX statement
2. **Clustering Information**: Adds ALTER TABLE ... CLUSTER commands if the index is used for clustering
3. **Statistics Settings**: Generates ALTER INDEX ... SET STATISTICS commands for columns with custom statistics targets
4. **Replica Identity**: Sets replica identity using the index if configured
5. **Constraint Handling**: For constraint-backed indexes, only dumps comments (not the index itself, as it's handled by the constraint)
6. **Extension Dependencies**: Records dependencies on extensions
7. **Partitioned Index Handling**: Avoids generating DROP statements for partitioned index members

The function follows PostgreSQL's dump architecture by creating both creation and deletion statements, then archiving them with appropriate metadata.

## Parameters / Member Variables
- `*fout`: Archive pointer containing dump options and output context
- `*indxinfo`: IndxInfo structure containing index metadata including:
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [fmtId](../f/fmtId.md)  
  - fmtQualifiedDumpable
  - [binary_upgrade_set_pg_class_oids](../b/binary_upgrade_set_pg_class_oids.md)
  - [parsePGArray](../p/parsePGArray.md)
  - [append_depends_on_extension](../a/append_depends_on_extension.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Skips processing entirely in data-only dump mode
- [Constraint](../C/Constraint.md)-backed indexes only have their comments dumped, not the index definition itself
- Partitioned index members cannot be dropped independently, so no DROP statement is generated for them
- Binary upgrade mode requires special handling for object OID preservation
- Index statistics are parsed from PostgreSQL array format and converted to individual ALTER INDEX commands
- The function maintains synchronization with similar code in dumpConstraint for consistency

## Simplified Source

```c
static void
dumpIndex(Archive *fout, const IndxInfo *indxinfo)
{
    DumpOptions *dopt = fout->dopt;
    TableInfo *tbinfo = indxinfo->indextable;
    bool is_constraint = (indxinfo->indexconstraint != 0);
    PQExpBuffer q, delq;
    char *qindxname, *qqindxname;

    // Skip if data-only dump
    if (dopt->dataOnly)
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();

    qindxname = pg_strdup(fmtId(indxinfo->dobj.name));
    qqindxname = pg_strdup(fmtQualifiedDumpable(indxinfo));

    // Only dump standalone indexes (not constraint-backed ones)
    if (!is_constraint)
    {
        // Binary upgrade OID handling
        if (dopt->binary_upgrade)
            binary_upgrade_set_pg_class_oids(fout, q, indxinfo->dobj.catId.oid, true);

        // Basic index creation
        appendPQExpBuffer(q, "%s;\n", indxinfo->indexdef);

        // Handle clustering
        if (indxinfo->indisclustered)
        {
            appendPQExpBuffer(q, "\nALTER TABLE %s CLUSTER ON %s;\n",
                            fmtQualifiedDumpable(tbinfo), qindxname);
        }

        // Handle index statistics
        if (strlen(indxinfo->indstatcols) != 0 || strlen(indxinfo->indstatvals) != 0)
        {
            char **indstatcolsarray = NULL;
            char **indstatvalsarray = NULL;
            int nstatcols = 0, nstatvals = 0;

            // Parse statistics arrays
            if (!parsePGArray(indxinfo->indstatcols, &indstatcolsarray, &nstatcols))
                pg_fatal("could not parse index statistic columns");
            if (!parsePGArray(indxinfo->indstatvals, &indstatvalsarray, &nstatvals))
                pg_fatal("could not parse index statistic values");

            // Generate ALTER INDEX SET STATISTICS commands
            for (int j = 0; j < nstatcols; j++)
            {
                appendPQExpBuffer(q, "ALTER INDEX %s ALTER COLUMN %s SET STATISTICS %s;\n",
                                qqindxname, indstatcolsarray[j], indstatvalsarray[j]);
            }

            free(indstatcolsarray);
            free(indstatvalsarray);
        }

        // Handle extension dependencies
        append_depends_on_extension(fout, q, &indxinfo->dobj,
                                   "pg_catalog.pg_class", "INDEX", qqindxname);

        // Handle replica identity
        if (indxinfo->indisreplident)
        {
            appendPQExpBuffer(q, "\nALTER TABLE ONLY %s REPLICA IDENTITY USING INDEX %s;\n",
                            fmtQualifiedDumpable(tbinfo), qindxname);
        }

        // Generate DROP statement (only for non-partitioned index members)
        if (indxinfo->parentidx == 0)
            appendPQExpBuffer(delq, "DROP INDEX %s;\n", qqindxname);

        // Create archive entry
        if (indxinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
            ArchiveEntry(fout, indxinfo->dobj.catId, indxinfo->dobj.dumpId,
                        ARCHIVE_OPTS(.tag = indxinfo->dobj.name,
                                    .namespace = tbinfo->dobj.namespace->dobj.name,
                                    .tablespace = indxinfo->tablespace,
                                    .owner = tbinfo->rolname,
                                    .description = "INDEX",
                                    .section = SECTION_POST_DATA,
                                    .createStmt = q->data,
                                    .dropStmt = delq->data));
    }

    // Dump index comments
    if (indxinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "INDEX", qindxname,
                   tbinfo->dobj.namespace->dobj.name,
                   tbinfo->rolname,
                   indxinfo->dobj.catId, 0,
                   is_constraint ? indxinfo->indexconstraint : indxinfo->dobj.dumpId);

    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    free(qindxname);
    free(qqindxname);
}
```