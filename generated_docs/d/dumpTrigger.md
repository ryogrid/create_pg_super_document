# dumpTrigger

## Location
src/bin/pg_dump/pg_dump.c: 17893 - 18018

## Overview
Writes the SQL declaration of one user-defined table trigger, including special handling for partition triggers and trigger enable/disable states.

## Definition


## Detailed Description
The  function generates SQL CREATE TRIGGER statements and associated ALTER TRIGGER commands to restore trigger definitions and their enabled states. It handles regular triggers by using the stored trigger definition (), and processes partition triggers specially by generating ALTER TABLE statements to modify the trigger's enabled state rather than recreating it. The function also manages trigger dependencies on extensions and generates appropriate DROP TRIGGER statements for cleanup. For triggers that are not in the default enabled state ('t' or 'O'), it appends ALTER TABLE commands to set the correct enabled state (DISABLE, ENABLE ALWAYS, ENABLE REPLICA).

## Parameters / Member Variables
- : Archive structure containing dump options and output methods
- : TriggerInfo structure containing trigger metadata including definition, enabled state, and partition status

## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [append_depends_on_extension](../a/append_depends_on_extension.md)
  - createPQExpBuffer/resetPQExpBuffer/destroyPQExpBuffer
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