# dumpTrigger

## Location
[src/bin/pg_dump/pg_dump.c:17893-18018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L17893-L18018)

## Overview
Writes the SQL declaration of one user-defined table trigger, including special handling for partition triggers and trigger enable/disable states.

## Definition

```c
static void
dumpTrigger(Archive *fout, const TriggerInfo *tginfo)
```
## Detailed Description
The  function generates SQL CREATE TRIGGER statements and associated ALTER TRIGGER commands to restore trigger definitions and their enabled states. It handles regular triggers by using the stored trigger definition (), and processes partition triggers specially by generating ALTER TABLE statements to modify the trigger's enabled state rather than recreating it. The function also manages trigger dependencies on extensions and generates appropriate DROP TRIGGER statements for cleanup. For triggers that are not in the default enabled state ('t' or 'O'), it appends ALTER TABLE commands to set the correct enabled state (DISABLE, ENABLE ALWAYS, ENABLE REPLICA).

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*tginfo`: TriggerInfo structure containing trigger metadata including definition, enabled state, and partition status
## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [append_depends_on_extension](../a/append_depends_on_extension.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/resetPQExpBuffer/destroyPQExpBuffer
  - [psprintf](../p/psprintf.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Skips processing entirely in data-only dump mode
- Partition triggers receive special treatment: instead of CREATE TRIGGER, generates ALTER TABLE statements to modify trigger enabled state
- Handles four trigger enabled states: 'f'/'D' (disabled), 't'/'O' (enabled), 'R' (replica), 'A' (always)
- Creates archive entries in SECTION_POST_DATA to ensure triggers are created after tables and their data
- Supports both regular tables and foreign tables (RELKIND_FOREIGN_TABLE)
- Generates extension dependencies automatically for triggers that depend on extensions
- Creates proper DROP statements for use during pg_dump --create operations

## Simplified Source

```c
static void
dumpTrigger(Archive *fout, const TriggerInfo *tginfo)
{
    DumpOptions *dopt = fout->dopt;
    TableInfo *tbinfo = tginfo->tgtable;
    PQExpBuffer query, delqry, trigprefix, trigidentity;
    char *qtabname, *tag;

    // Skip in data-only mode
    if (dopt->dataOnly)
        return;

    query = createPQExpBuffer();
    delqry = createPQExpBuffer();
    trigprefix = createPQExpBuffer();
    trigidentity = createPQExpBuffer();

    qtabname = pg_strdup(fmtId(tbinfo->dobj.name));

    // Build trigger identity string
    appendPQExpBuffer(trigidentity, "%s ", fmtId(tginfo->dobj.name));
    appendPQExpBuffer(trigidentity, "ON %s", fmtQualifiedDumpable(tbinfo));

    // Use stored trigger definition
    appendPQExpBuffer(query, "%s;\n", tginfo->tgdef);
    appendPQExpBuffer(delqry, "DROP TRIGGER %s;\n", trigidentity->data);

    // Add extension dependencies if needed
    append_depends_on_extension(fout, query, &tginfo->dobj,
                               "pg_catalog.pg_trigger", "TRIGGER",
                               trigidentity->data);

    if (tginfo->tgispartition) {
        // Handle partition triggers: ALTER instead of CREATE
        Assert(tbinfo->ispartition);

        resetPQExpBuffer(query);
        resetPQExpBuffer(delqry);
        appendPQExpBuffer(query, "\nALTER %sTABLE %s ",
                         tbinfo->relkind == RELKIND_FOREIGN_TABLE ? "FOREIGN " : "",
                         fmtQualifiedDumpable(tbinfo));

        // Set appropriate enable state
        switch (tginfo->tgenabled) {
            case 'f':
            case 'D':
                appendPQExpBufferStr(query, "DISABLE");
                break;
            case 't':
            case 'O':
                appendPQExpBufferStr(query, "ENABLE");
                break;
            case 'R':
                appendPQExpBufferStr(query, "ENABLE REPLICA");
                break;
            case 'A':
                appendPQExpBufferStr(query, "ENABLE ALWAYS");
                break;
        }
        appendPQExpBuffer(query, " TRIGGER %s;\n", fmtId(tginfo->dobj.name));
    }
    else if (tginfo->tgenabled != 't' && tginfo->tgenabled != 'O') {
        // Handle non-default enabled states for regular triggers
        appendPQExpBuffer(query, "\nALTER %sTABLE %s ",
                         tbinfo->relkind == RELKIND_FOREIGN_TABLE ? "FOREIGN " : "",
                         fmtQualifiedDumpable(tbinfo));

        switch (tginfo->tgenabled) {
            case 'D':
            case 'f':
                appendPQExpBufferStr(query, "DISABLE");
                break;
            case 'A':
                appendPQExpBufferStr(query, "ENABLE ALWAYS");
                break;
            case 'R':
                appendPQExpBufferStr(query, "ENABLE REPLICA");
                break;
            default:
                appendPQExpBufferStr(query, "ENABLE");
                break;
        }
        appendPQExpBuffer(query, " TRIGGER %s;\n", fmtId(tginfo->dobj.name));
    }

    // Build comment prefix
    appendPQExpBuffer(trigprefix, "TRIGGER %s ON", fmtId(tginfo->dobj.name));

    tag = psprintf("%s %s", tbinfo->dobj.name, tginfo->dobj.name);

    // Create archive entry
    if (tginfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, tginfo->dobj.catId, tginfo->dobj.dumpId,
                   ARCHIVE_OPTS(.tag = tag,
                               .namespace = tbinfo->dobj.namespace->dobj.name,
                               .owner = tbinfo->rolname,
                               .description = "TRIGGER",
                               .section = SECTION_POST_DATA,
                               .createStmt = query->data,
                               .dropStmt = delqry->data));

    // Dump trigger comments
    if (tginfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, trigprefix->data, qtabname,
                   tbinfo->dobj.namespace->dobj.name, tbinfo->rolname,
                   tginfo->dobj.catId, 0, tginfo->dobj.dumpId);

    free(tag);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(delqry);
    destroyPQExpBuffer(trigprefix);
    destroyPQExpBuffer(trigidentity);
    free(qtabname);
}
```