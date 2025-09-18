# parseArchiveFormat

## Location
[src/bin/pg_dump/pg_dump.c:1411-1448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1411-L1448)

## Overview
Parses a command-line archive format string and converts it into the corresponding ArchiveFormat enum value, while also setting the appropriate ArchiveMode.

## Definition


## Detailed Description
This function is responsible for parsing the archive format specification provided by the user through command-line arguments (typically via the -F option in pg_dump). It performs case-insensitive string comparison to match the input format string against known format types and returns the corresponding ArchiveFormat enum value. The function also sets the appropriate ArchiveMode through the mode output parameter.

The function supports both short and long format names:
- 'a' or 'append': Sets archNull format with archModeAppend (used internally by pg_dumpall)
- 'c' or 'custom': Sets archCustom format
- 'd' or 'directory': Sets archDirectory format  
- 'p' or 'plain': Sets archNull format with archModeWrite
- 't' or 'tar': Sets archTar format

If an unrecognized format is provided, the function terminates the program with a fatal error.

## Parameters / Member Variables
- : Input string specifying the desired archive format (case-insensitive)
- : Output parameter that receives the corresponding ArchiveMode (archModeWrite or archModeAppend)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](pg_strcasecmp.md) (string comparison)
  - [pg_fatal](pg_fatal.md) (error handling)
  - [ArchiveFormat](../A/ArchiveFormat.md) enum values: archNull, archCustom, archDirectory, archTar
  - [ArchiveMode](../A/ArchiveMode.md) enum values: archModeWrite, archModeAppend
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c at line 756)
  - fmtQualifiedDumpable (in pg_dump.c at line 185)

## Notes and Other Information
- The 'append' format is specifically noted as being used by pg_dumpall and is not documented in user-facing documentation
- The function uses case-insensitive comparison, allowing users to specify formats in any case
- Both single-character and full-word format specifications are supported for user convenience
- Error handling is immediate and fatal - invalid formats cause program termination rather than returning an error code