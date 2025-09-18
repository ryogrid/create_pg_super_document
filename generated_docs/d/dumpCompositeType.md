# dumpCompositeType

## Location
[src/bin/pg_dump/pg_dump.c:11787-11992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L11787-L11992)

## Overview
The dumpCompositeType function generates SQL statements to recreate a user-defined composite type during PostgreSQL database dumps.

## Definition


## Detailed Description
This function processes a composite type (user-defined type with multiple attributes) and generates the appropriate CREATE TYPE statement along with any necessary metadata. It handles both regular dumps and binary upgrades, with special consideration for dropped columns in binary upgrade mode.

The function performs a query to retrieve all attributes of the composite type, including their names, types, alignment, length, and collation information. It constructs a CREATE TYPE statement with all non-dropped attributes, and for binary upgrades, it includes special handling for dropped columns by creating placeholders and generating subsequent ALTER statements.

The function also handles dumping of associated comments, security labels, and access control lists for the type and its columns.

## Parameters / Member Variables
- : Archive handle for the dump output stream
- : TypeInfo structure containing metadata about the composite type to dump

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - [binary_upgrade_set_pg_class_oids](../b/binary_upgrade_set_pg_class_oids.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [findCollationByOid](../f/findCollationByOid.md)
  - appendStringLiteralAH
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
  - [dumpCompositeTypeColComments](dumpCompositeTypeColComments.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Uses prepared statements for efficiency when dumping multiple composite types
- Special handling for binary upgrade mode includes preserving dropped columns as placeholders
- Collation clauses are only included when they differ from the type's default collation
- The function dumps the type definition in the PRE_DATA section to ensure proper dependency ordering
- Column comments are handled separately via dumpCompositeTypeColComments