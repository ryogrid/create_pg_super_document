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