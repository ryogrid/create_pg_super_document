# dumpPolicy

## Location
[src/bin/pg_dump/pg_dump.c:4117-4234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4117-L4234)

## Overview
Generates the SQL statements to recreate a Row-Level Security policy or RLS enablement for a table during pg_dump restore operations.

## Definition

```c
static void
dumpPolicy(Archive *fout, const PolicyInfo *polinfo)
```
## Detailed Description
The `dumpPolicy` function creates the SQL DDL statements needed to recreate security policies during database restoration. It handles two distinct cases:

1. **RLS Enablement**: When `polinfo->polname` is NULL, it generates an "ALTER TABLE ... ENABLE ROW LEVEL SECURITY" statement to enable RLS on the table.

2. **Actual Policies**: For named policies, it constructs a "CREATE POLICY" statement with all the policy attributes including:
   - Policy name and target table
   - Command type (SELECT, INSERT, UPDATE, DELETE, or ALL)
   - Restrictive vs. permissive nature
   - Target roles
   - USING qualifier expression
   - WITH CHECK expression

The function also generates the corresponding DROP statement for cleanup during restoration and handles comments and security labels if present.

## Parameters / Member Variables
- `fout`: Archive pointer for output operations and dump options
- `polinfo`: PolicyInfo structure containing policy details including:
  - `polname`: Policy name (NULL for RLS enablement)
  - `poltable`: Associated TableInfo structure
  - `polcmd`: Policy command type ('*', 'r', 'a', 'w', 'd')
  - `polpermissive`: Whether policy is permissive (vs restrictive)
  - `polroles`: Target roles for the policy
  - `polqual`: USING clause expression
  - `polwithcheck`: WITH CHECK clause expression

## Dependencies
- Functions called/Symbols referenced:
  - `DumpOptions`, `TableInfo` (data structures)
  - `[createPQExpBuffer](../c/createPQExpBuffer.md)`, `appendPQExpBuffer` series (query building)
  - `fmtQualifiedDumpable`, `fmtId` (identifier formatting)
  - [ArchiveEntry](../A/ArchiveEntry.md) (archive entry creation)
  - [dumpComment](dumpComment.md), `dumpSecLabel` (auxiliary object dumping)
  - `DUMP_COMPONENT_DEFINITION`, `SECTION_POST_DATA` (component flags)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (main dump dispatch function)

## Notes and Other Information
- Skips processing entirely in data-only dump mode
- Maps policy command characters to SQL keywords: 'r'→SELECT, 'a'→INSERT, 'w'→UPDATE, 'd'→DELETE, '*'→ALL
- Creates archive entries in SECTION_POST_DATA to ensure policies are created after tables
- Handles both restrictive (default) and permissive policies (PostgreSQL 10+)
- Generates qualified table names to handle cross-schema references correctly
- Part of the comprehensive database object recreation system in pg_dump/pg_restore

## Simplified Source

```c
static void
dumpPolicy(Archive *fout, const PolicyInfo *polinfo)
{
    DumpOptions *dopt = fout->dopt;
    TableInfo *tbinfo = polinfo->poltable;

    // Skip in data-only dumps
    if (dopt->dataOnly)
        return;

    // Handle RLS enablement (polname = NULL)
    if (polinfo->polname == NULL) {
        PQExpBuffer query = createPQExpBuffer();

        appendPQExpBuffer(query, "ALTER TABLE %s ENABLE ROW LEVEL SECURITY;",
                         fmtQualifiedDumpable(tbinfo));

        if (polinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
            ArchiveEntry(fout, polinfo->dobj.catId, polinfo->dobj.dumpId,
                        ARCHIVE_OPTS(.tag = polinfo->dobj.name,
                                    .namespace = polinfo->dobj.namespace->dobj.name,
                                    .owner = tbinfo->rolname,
                                    .description = "ROW SECURITY",
                                    .section = SECTION_POST_DATA,
                                    .createStmt = query->data,
                                    .deps = &(tbinfo->dobj.dumpId),
                                    .nDeps = 1));

        destroyPQExpBuffer(query);
        return;
    }

    // Map policy command character to SQL clause
    const char *cmd;
    switch (polinfo->polcmd) {
        case '*': cmd = ""; break;                    // ALL commands
        case 'r': cmd = " FOR SELECT"; break;
        case 'a': cmd = " FOR INSERT"; break;
        case 'w': cmd = " FOR UPDATE"; break;
        case 'd': cmd = " FOR DELETE"; break;
        default:
            pg_fatal("unexpected policy command type: %c", polinfo->polcmd);
    }

    PQExpBuffer query = createPQExpBuffer();
    PQExpBuffer delqry = createPQExpBuffer();
    char *qtabname = pg_strdup(fmtId(tbinfo->dobj.name));

    // Build CREATE POLICY statement
    appendPQExpBuffer(query, "CREATE POLICY %s", fmtId(polinfo->polname));
    appendPQExpBuffer(query, " ON %s%s%s", fmtQualifiedDumpable(tbinfo),
                     !polinfo->polpermissive ? " AS RESTRICTIVE" : "", cmd);

    if (polinfo->polroles != NULL)
        appendPQExpBuffer(query, " TO %s", polinfo->polroles);

    if (polinfo->polqual != NULL)
        appendPQExpBuffer(query, " USING (%s)", polinfo->polqual);

    if (polinfo->polwithcheck != NULL)
        appendPQExpBuffer(query, " WITH CHECK (%s)", polinfo->polwithcheck);

    appendPQExpBufferStr(query, ";\n");

    // Build DROP POLICY statement
    appendPQExpBuffer(delqry, "DROP POLICY %s ON %s;\n",
                     fmtId(polinfo->polname), fmtQualifiedDumpable(tbinfo));

    // Create archive entry
    char *tag = psprintf("%s %s", tbinfo->dobj.name, polinfo->dobj.name);

    if (polinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, polinfo->dobj.catId, polinfo->dobj.dumpId,
                    ARCHIVE_OPTS(.tag = tag,
                                .namespace = polinfo->dobj.namespace->dobj.name,
                                .owner = tbinfo->rolname,
                                .description = "POLICY",
                                .section = SECTION_POST_DATA,
                                .createStmt = query->data,
                                .dropStmt = delqry->data));

    // Dump policy comments if requested
    if (polinfo->dobj.dump & DUMP_COMPONENT_COMMENT) {
        PQExpBuffer polprefix = createPQExpBuffer();
        appendPQExpBuffer(polprefix, "POLICY %s ON", fmtId(polinfo->polname));
        dumpComment(fout, polprefix->data, qtabname,
                   tbinfo->dobj.namespace->dobj.name, tbinfo->rolname,
                   polinfo->dobj.catId, 0, polinfo->dobj.dumpId);
        destroyPQExpBuffer(polprefix);
    }

    // Cleanup
    free(tag);
    free(qtabname);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(delqry);
}
```