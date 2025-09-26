# dumpProcLang

## Location
[src/bin/pg_dump/pg_dump.c:12128-12259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L12128-L12259)

## Overview
The dumpProcLang function generates SQL statements to recreate a user-defined procedural language during PostgreSQL database dumps.

## Definition

```c
static void
dumpProcLang(Archive *fout, const ProcLangInfo *plang)
```
## Detailed Description
This function processes a procedural language definition and generates the appropriate CREATE PROCEDURAL LANGUAGE statement. It handles two different scenarios: when the language's support functions are available for dumping (creating a complete definition with parameters), and when they are not (creating a parameterless definition that relies on extension templates).

The function searches for the language's handler, inline, and validator functions. If all required functions are found and dumpable, it creates a complete CREATE PROCEDURAL LANGUAGE statement with all parameters. Otherwise, it creates a CREATE OR REPLACE PROCEDURAL LANGUAGE statement without parameters, which modern servers interpret as CREATE EXTENSION IF NOT EXISTS.

The function also handles dumping of associated comments, security labels, and access control lists. For trusted languages, ACL information is included in the dump.

## Parameters / Member Variables
- : Archive handle for the dump output stream
- : ProcLangInfo structure containing metadata about the procedural language

## Dependencies
- Functions called/Symbols referenced:
  - [findFuncByOid](../f/findFuncByOid.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Returns early if dataOnly dump mode is specified since languages are schema constructs
- Attempts to locate handler, inline, and validator functions by OID
- Uses parameterless CREATE OR REPLACE when support functions are not dumpable
- Includes TRUSTED keyword when the language is marked as trusted
- ACL dumping is conditional on the language being trusted
- Archived in SECTION_PRE_DATA to ensure proper dependency ordering
- In binary upgrade mode, handles extension membership properly
- Modern servers interpret parameterless commands as extension creation