# dumpUndefinedType

## Location
[src/bin/pg_dump/pg_dump.c:11249-11312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L11249-L11312)

## Overview
Generates SQL commands to recreate an undefined shell type (where typisdefined is false) during PostgreSQL database dump operations.

## Definition

```c
static void
dumpUndefinedType(Archive *fout, const TypeInfo *tyinfo)
```
## Detailed Description
The  function handles the dumping of undefined types, also known as shell types that have not been fully defined. These are types that exist in the system catalogs but lack complete definition (typisdefined = false). This is distinct from shell types created temporarily to break circular dependencies - undefined types are genuine incomplete type definitions that shouldn't have any dependencies.

The function creates a simple  statement without any implementation details, effectively creating a shell type that can be completed later. This is used for types that were created but never fully defined in the source database.

## Parameters / Member Variables
- `*fout`: Archive object containing dump configuration and state information
- `*tyinfo`: TypeInfo structure containing metadata about the undefined type to be dumped
## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Creates shell types using simple  syntax without any parameters
- Handles binary upgrade mode with OID preservation for consistency
- Unlike other type dump functions, this doesn't require complex queries to system catalogs since undefined types have minimal metadata
- Undefined types should not have dependencies, making them safe to dump as simple shell types
- Includes full dump component handling for comments, security labels, and ACLs despite the type being undefined
- The distinction from dependency-breaking shell types is important for understanding PostgreSQL's type system architecture