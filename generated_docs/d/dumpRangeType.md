# dumpRangeType

## Location
[src/bin/pg_dump/pg_dump.c:11091-11248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L11091-L11248)

## Overview
Generates SQL commands to recreate a user-defined range type during PostgreSQL database dump operations.

## Definition

```c
static void
dumpRangeType(Archive *fout, const TypeInfo *tyinfo)
```
## Detailed Description
The  function creates SQL statements to recreate user-defined range types in PostgreSQL dumps. Range types are composite types that represent a range of values of some element type (the subtype). The function handles complex range type properties including subtype, operator class, collation, canonical function, and subtype difference function.

The function performs the following operations:
1. Prepares and executes a query to retrieve range type metadata from , , and  system catalogs
2. Constructs a  statement with appropriate parameters
3. Handles version-specific features like multirange types (PostgreSQL 14+)
4. Manages optional parameters like custom operator classes, collations, canonical functions, and subtype difference functions
5. Supports binary upgrade mode with OID preservation

## Parameters / Member Variables
- : Archive object containing dump configuration and state information
- : TypeInfo structure containing metadata about the range type to be dumped

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - [findCollationByOid](../f/findCollationByOid.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Uses prepared statements for efficiency when dumping multiple range types
- Supports PostgreSQL 14+ multirange types with version-specific conditional logic
- Only includes non-default operator classes in the CREATE TYPE statement for brevity
- Handles collation specifications only when different from the subtype's default collation
- Optional canonical and subtype_diff functions are included only when explicitly defined
- Binary upgrade mode preserves original type OIDs for consistent restoration
- Comprehensive dump component handling for definition, comments, security labels, and ACLs