# dumpEnumType

## Location
[src/bin/pg_dump/pg_dump.c:10951-11090](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L10951-L11090)

## Overview
Generates SQL commands to recreate a user-defined enum type during PostgreSQL database dump operations.

## Definition


## Detailed Description
The  function is responsible for creating SQL statements that recreate user-defined enumerated types in PostgreSQL dumps. It handles both regular dumps and binary upgrade scenarios, ensuring that enum values are recreated with the correct order and, in binary upgrade mode, with preserved OIDs.

The function performs the following key operations:
1. Prepares and executes a query to retrieve enum labels from  ordered by 
2. Constructs a  statement with the enum values
3. For binary upgrades, preserves original OIDs using 
4. Handles associated metadata including comments, security labels, and ACLs

## Parameters / Member Variables
- : Archive object containing dump configuration and state information
- : TypeInfo structure containing metadata about the enum type to be dumped

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - appendStringLiteralAH
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Uses prepared statements for efficiency when dumping multiple enum types
- Binary upgrade mode requires special handling to preserve enum value OIDs
- The function ensures proper SQL escaping of enum labels using 
- Includes comprehensive dump component handling (definition, comments, security labels, ACLs)
- Enum values are retrieved in  to maintain proper ordering in the recreated type