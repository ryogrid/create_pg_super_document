# dumpEventTrigger

## Location
[src/bin/pg_dump/pg_dump.c:18019-18103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18019-L18103)

## Overview
Writes the SQL declaration of one user-defined event trigger, including its event type, optional tag filters, and enabled state.

## Definition

```c
static void
dumpEventTrigger(Archive *fout, const EventTriggerInfo *evtinfo)
```
## Detailed Description
The  function generates SQL CREATE EVENT TRIGGER statements to restore PostgreSQL event triggers. Event triggers fire on database-wide events (like DDL commands) rather than table-specific events. The function constructs the CREATE EVENT TRIGGER statement with the trigger name, event type, optional WHEN TAG IN clause for filtering specific DDL commands, and the EXECUTE FUNCTION clause specifying the trigger function. If the event trigger is not in the default enabled state ('O'), it appends ALTER EVENT TRIGGER statements to set the correct enabled state (DISABLE, ENABLE, ENABLE ALWAYS, ENABLE REPLICA).

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*evtinfo`: EventTriggerInfo structure containing event trigger metadata including name, event type, tags filter, function name, owner, and enabled state
## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/destroyPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)/appendPQExpBufferChar
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Skips processing entirely in data-only dump mode
- Handles optional TAG filtering through the WHEN TAG IN clause if evttags is not empty
- Supports four enabled states: 'D' (disabled), 'A' (always), 'R' (replica), 'O' (normal/default)
- Creates archive entries in SECTION_POST_DATA to ensure event triggers are created after other database objects
- Event triggers are database-wide objects (no namespace) but have owners
- Supports binary upgrade mode for preserving extension membership
- Uses NULL namespace in dumpComment call since event triggers are not schema-scoped objects

## Simplified Source

```c
static void
dumpEventTrigger(Archive *fout, const EventTriggerInfo *evtinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query, delqry;
    char *qevtname;

    // Skip in data-only mode
    if (dopt->dataOnly)
        return;

    query = createPQExpBuffer();
    delqry = createPQExpBuffer();

    qevtname = pg_strdup(fmtId(evtinfo->dobj.name));

    // Build CREATE EVENT TRIGGER statement
    appendPQExpBufferStr(query, "CREATE EVENT TRIGGER ");
    appendPQExpBufferStr(query, qevtname);
    appendPQExpBufferStr(query, " ON ");
    appendPQExpBufferStr(query, fmtId(evtinfo->evtevent));

    // Add optional WHEN TAG IN clause
    if (strcmp("", evtinfo->evttags) != 0) {
        appendPQExpBufferStr(query, "\n         WHEN TAG IN (");
        appendPQExpBufferStr(query, evtinfo->evttags);
        appendPQExpBufferChar(query, ')');
    }

    // Add EXECUTE FUNCTION clause
    appendPQExpBufferStr(query, "\n   EXECUTE FUNCTION ");
    appendPQExpBufferStr(query, evtinfo->evtfname);
    appendPQExpBufferStr(query, "();\n");

    // Handle non-default enabled states
    if (evtinfo->evtenabled != 'O') {
        appendPQExpBuffer(query, "\nALTER EVENT TRIGGER %s ", qevtname);
        switch (evtinfo->evtenabled) {
            case 'D':
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
        appendPQExpBufferStr(query, ";\n");
    }

    // Build DROP statement
    appendPQExpBuffer(delqry, "DROP EVENT TRIGGER %s;\n", qevtname);

    // Handle binary upgrade extensions
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(query, &evtinfo->dobj,
                                       "EVENT TRIGGER", qevtname, NULL);

    // Create archive entry
    if (evtinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, evtinfo->dobj.catId, evtinfo->dobj.dumpId,
                   ARCHIVE_OPTS(.tag = evtinfo->dobj.name,
                               .owner = evtinfo->evtowner,
                               .description = "EVENT TRIGGER",
                               .section = SECTION_POST_DATA,
                               .createStmt = query->data,
                               .dropStmt = delqry->data));

    // Dump comments (no namespace for event triggers)
    if (evtinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "EVENT TRIGGER", qevtname,
                   NULL, evtinfo->evtowner,
                   evtinfo->dobj.catId, 0, evtinfo->dobj.dumpId);

    destroyPQExpBuffer(query);
    destroyPQExpBuffer(delqry);
    free(qevtname);
}
```