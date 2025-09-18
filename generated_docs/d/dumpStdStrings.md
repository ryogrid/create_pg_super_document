# dumpStdStrings

## Location
[src/bin/pg_dump/pg_dump.c:3590-3613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L3590-L3613)

## Overview
The  function saves the database's standard_conforming_strings setting to the archive to ensure proper string literal handling during restoration.

## Definition


## Detailed Description
The  function creates an archive entry that contains a SQL command to set the standard_conforming_strings parameter to match the source database's setting. This parameter controls whether backslashes in string literals are treated as escape characters or literal backslashes. The function reads the std_strings boolean flag from the archive structure, converts it to the appropriate 'on' or 'off' string value, and creates a SET command that will be executed during restoration in the PRE_DATA section.

## Parameters / Member Variables
- : Pointer to the Archive structure containing the std_strings boolean flag that indicates the database's standard_conforming_strings setting

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info (logs the setting being saved)
  - createPQExpBuffer/appendPQExpBuffer/destroyPQExpBuffer (string buffer management)
  - [createDumpId](../c/createDumpId.md) (generates unique dump ID)
  - [ArchiveEntry](../A/ArchiveEntry.md) (creates archive entry with SQL command)
  - ARCHIVE_OPTS/SECTION_PRE_DATA (archive configuration macros)
- Called from (representative examples):
  - [main](../m/main.md) (pg_dump main function)
  - fmtQualifiedDumpable

## Notes and Other Information
- This setting is critical for proper handling of backslash escapes in string literals
- The entry is placed in SECTION_PRE_DATA to ensure it's set before any data containing string literals is restored
- When standard_conforming_strings is 'on', backslashes are treated as literal characters
- When 'off', backslashes act as escape characters (legacy behavior)
- This ensures compatibility when restoring dumps between databases with different default settings