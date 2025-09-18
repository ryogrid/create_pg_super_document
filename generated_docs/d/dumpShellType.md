# dumpShellType

## Location
[src/bin/pg_dump/pg_dump.c:12082-12127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L12082-L12127)

## Overview
The dumpShellType function generates a CREATE TYPE statement for a shell type, which is a placeholder type definition created before the actual type implementation.

## Definition


## Detailed Description
This function creates a shell type definition, which is an incomplete type declaration that reserves the type name before its full definition (including I/O functions) is available. Shell types are essential for handling circular dependencies in PostgreSQL type definitions, particularly when types need to reference each other.

The function generates a simple CREATE TYPE statement without any implementation details. Notably, it does not generate a corresponding DROP statement, as the cleanup is handled by the base type entry. This design choice prevents premature ownership changes that could cause backend complaints before the type is fully defined.

## Parameters / Member Variables
- : Archive handle for the dump output stream
- : ShellTypeInfo structure containing metadata about the shell type to create

## Dependencies
- Functions called/Symbols referenced:
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - fmtQualifiedDumpable
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - createPQExpBuffer
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - destroyPQExpBuffer
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Returns early if dataOnly dump mode is specified since shell types are schema-only constructs
- In binary upgrade mode, preserves the original type OID for consistency
- Does not generate DROP statements; cleanup is managed by the associated base type
- Owner changes are deferred until after the type is fully implemented to avoid backend errors
- Archived in SECTION_PRE_DATA to ensure proper dependency ordering
- The shell type's owner is derived from the base type's role name