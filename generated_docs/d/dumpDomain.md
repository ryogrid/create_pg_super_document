# dumpDomain

## Location
src/bin/pg_dump/pg_dump.c: 11562 - 11786

## Overview
Generates SQL commands to recreate a user-defined domain type with constraints, defaults, and collations during PostgreSQL database dump operations.

## Definition


## Detailed Description
The  function creates SQL statements to recreate domain types in PostgreSQL dumps. Domains are essentially constrained versions of existing data types, allowing users to define reusable type definitions with specific constraints, default values, and collations. The function handles the complete domain specification including base type, collation (when different from base type), NOT NULL constraints (with version-specific naming), default values, and CHECK constraints.

The function performs the following operations:
1. Queries  system catalog for domain metadata including base type, constraints, defaults, and collation information
2. Constructs a  statement with all applicable modifiers
3. Handles version-specific features like named NOT NULL constraints (PostgreSQL 17+)
4. Includes custom collations only when they differ from the base type's collation
5. Processes inline CHECK constraints and NOT NULL constraints
6. Manages both literal and expression-based default values
7. Dumps comments for individual constraints

## Parameters / Member Variables
- : Archive object containing dump configuration and state information
- : TypeInfo structure containing metadata about the domain type to be dumped, including constraint information

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - [findCollationByOid](../f/findCollationByOid.md)
  - appendStringLiteralAH
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Supports sophisticated constraint handling including inline CHECK constraints and NOT NULL constraints
- Version-aware NOT NULL constraint naming (PostgreSQL 17+ supports named NOT NULL constraints)
- Only includes collation specification when it differs from the base type to avoid redundancy
- Handles both compiled expressions (typdefaultbin) and literal defaults (typdefault) appropriately
- Binary upgrade mode forces array type creation for domains
- Comprehensive constraint comment handling for both CHECK and NOT NULL constraints
- Uses 'DOMAIN' description in archive entries to distinguish from other type categories
- The function demonstrates PostgreSQL's evolution with conditional logic for newer constraint features