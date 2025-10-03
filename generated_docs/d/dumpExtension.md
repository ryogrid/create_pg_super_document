# dumpExtension

## Location
[src/bin/pg_dump/pg_dump.c:10792-10919](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L10792-L10919)

## Overview
Generates SQL commands to recreate a PostgreSQL extension during database restore, with different strategies for regular dumps versus binary upgrade scenarios.

## Definition

```c
static void
dumpExtension(Archive *fout, const ExtensionInfo *extinfo)
```
## Detailed Description
The  function creates SQL statements to restore PostgreSQL extensions. It handles two distinct scenarios: regular dumps where it creates extensions using  allowing for flexible version handling, and binary upgrade mode where it precisely recreates the exact extension state including version, configuration, and dependencies.

In regular mode, the function intentionally omits version specification to use the destination installation's default version. In binary upgrade mode, it creates an empty extension with exact metadata and relies on  to add individual objects. The function also handles extension dependencies and configuration arrays while preserving OID relationships during binary upgrades.

## Parameters / Member Variables
- `*fout`: Archive structure representing the dump destination and containing connection/output information
- `*extinfo`: Pointer to ExtensionInfo structure containing extension metadata including name, namespace, version, configuration, condition, and dependencies
## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - appendStringLiteralAH
  - [findObjectByDumpId](../f/findObjectByDumpId.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (in pg_dump.c:10541)

## Notes and Other Information
- Skips processing entirely in data-only dump mode ()
- Uses  clause in regular mode to allow pre-existing extensions
- Binary upgrade mode calls  function
- Handles extension configuration arrays () and conditions () as-is during binary upgrade
- Processes extension dependencies, particularly other extensions in the dependency chain
- Supports dumping extension comments and security labels based on component flags
- Memory management includes proper cleanup of allocated resources and formatted strings