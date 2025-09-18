# dumpBaseType

## Location
[src/bin/pg_dump/pg_dump.c:11313-11561](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L11313-L11561)

## Overview
Generates SQL commands to recreate a user-defined base type with all its implementation details during PostgreSQL database dump operations.

## Definition


## Detailed Description
The  function creates comprehensive SQL statements to recreate user-defined base types in PostgreSQL dumps. Base types are the most complex type category, requiring complete specification of input/output functions, internal representation, storage characteristics, and operational behaviors. The function handles all aspects of base type definition including I/O functions, optional functions (receive/send, typmod, analyze, subscript), storage parameters, alignment, and behavioral attributes.

The function performs the following operations:
1. Queries  system catalog for comprehensive type metadata including all functions, storage parameters, and attributes
2. Constructs a detailed  statement with all required and optional parameters
3. Handles version-specific features like subscript functions (PostgreSQL 14+)
4. Manages type defaults, both literal and expression-based
5. Includes storage optimization parameters (alignment, storage mode, pass-by-value)
6. Supports element types for array base types
7. Handles type categories and preferences for operator resolution

## Parameters / Member Variables
- : Archive object containing dump configuration and state information
- : TypeInfo structure containing metadata about the base type to be dumped

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - appendStringLiteralAH
  - [getFormattedTypeName](../g/getFormattedTypeName.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Most complex type dump function due to the comprehensive nature of base type definitions
- Uses  because of circular dependencies between types and their I/O functions
- Handles variable-length types by converting typlen=-1 to 'variable'
- Only includes optional functions (receive/send, typmod, analyze, subscript) when they have valid OIDs
- Supports sophisticated default value handling with both literal strings and parsed expressions
- Includes comprehensive storage parameter specification (alignment, storage mode, pass-by-value)
- Type category 'U' (user-defined) is default and omitted for brevity
- Version-aware handling for newer features like subscript functions
- Full binary upgrade support with OID preservation for consistent restoration