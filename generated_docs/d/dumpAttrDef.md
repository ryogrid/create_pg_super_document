# dumpAttrDef

## Location
[src/bin/pg_dump/pg_dump.c:16876-16936](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L16876-L16936)

## Overview
Generates SQL commands to create column default value declarations using ALTER TABLE statements for columns that require separate default handling.

## Definition

```c
static void
dumpAttrDef(Archive *fout, const AttrDefInfo *adinfo)
```
## Detailed Description
This function creates ALTER TABLE ALTER COLUMN SET DEFAULT commands for column default values that need to be processed separately from the main table definition. It's specifically designed for default values that couldn't be included in the original CREATE TABLE statement, typically due to dependency ordering requirements.

The function generates both SET DEFAULT and DROP DEFAULT statements, handles foreign table specifics by including the FOREIGN keyword where appropriate, and creates proper archive entries for restoration. It only processes defaults marked as "separate" - defaults that were included in the main table definition are skipped to avoid duplication.

The function constructs human-readable tags combining table and column names for better identification in dump files and restoration logs.

## Parameters / Member Variables
- : Archive context containing dump configuration and output handling
- : Attribute default information including the default expression, associated table, column number, and separation flag

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [pg_strdup](../p/pg_strdup.md)
  - fmtQualifiedDumpable
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [fmtId](../f/fmtId.md)
  - [psprintf](../p/psprintf.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - free
  - destroyPQExpBuffer
- Types referenced:
  - [Archive](../A/Archive.md)
  - [AttrDefInfo](../A/AttrDefInfo.md)
  - DumpOptions
  - [TableInfo](../T/TableInfo.md)
  - PQExpBuffer
- Called from:
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Only processes defaults marked with the "separate" flag to avoid conflicts with table definition
- Skipped entirely in data-only dump mode since it's a schema operation
- Handles both regular and foreign tables with appropriate syntax
- Uses 1-based column numbering (adnum - 1) to access zero-based attnames array
- Creates both creation and deletion statements for complete restoration control
- Generates descriptive tags for better dump file organization and debugging
- Places commands in SECTION_PRE_DATA for proper restoration ordering