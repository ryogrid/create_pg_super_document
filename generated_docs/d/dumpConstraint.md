# dumpConstraint

## Location
src/bin/pg_dump/pg_dump.c: 17237 - 17548

## Overview
Writes out user-defined constraints to the dump archive, handling multiple constraint types including primary keys, unique constraints, foreign keys, and check constraints on both tables and domains.

## Definition


## Detailed Description
The  function is a comprehensive constraint dumping handler that generates appropriate SQL statements for different constraint types in PostgreSQL. It handles the complexity of constraint restoration by generating both creation and deletion statements with proper dependencies and metadata.

The function processes several constraint types:

1. **Primary Key and Unique Constraints ('p', 'u', 'x')**: 
   - Generates ALTER TABLE ADD CONSTRAINT statements
   - Handles NULLS NOT DISTINCT behavior
   - Includes INCLUDE columns for covering indexes
   - Processes storage options and deferrability settings
   - Manages clustering and replica identity settings

2. **Foreign Key Constraints ('f')**:
   - Creates ALTER TABLE ADD CONSTRAINT FOREIGN KEY statements
   - Handles partitioned tables vs regular tables differently (ONLY clause)
   - Uses pre-computed constraint definitions from pg_get_constraintdef

3. **Check Constraints ('c')**:
   - On tables: Creates ALTER TABLE ADD CONSTRAINT CHECK statements
   - On domains: Creates ALTER DOMAIN ADD CONSTRAINT CHECK statements
   - Only processes local, non-inherited constraints when dumping separately

4. **Not Null Constraints ('n')**:
   - On domains: Creates ALTER DOMAIN ADD CONSTRAINT statements

The function also handles special cases like binary upgrades, foreign tables, partitioned tables, and extension dependencies.

## Parameters / Member Variables
- : Archive pointer containing dump options and output context
- : ConstraintInfo structure containing:
  - Constraint type and name
  - Associated table or domain information
  - Constraint definition from catalog
  - Index information (for PK/UNIQUE constraints)
  - Deferrability and inheritance settings
  - Dump flags for component control

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - findObjectByDumpId
  - binary_upgrade_set_pg_class_oids
  - fmtQualifiedDumpable
  - fmtId
  - getAttrName
  - nonemptyReloptions
  - appendReloptionsArrayAH
  - append_depends_on_extension
  - ArchiveEntry
  - dumpTableConstraintComment
  - dumpComment
- Called from (representative examples):
  - dumpDumpableObject

## Notes and Other Information
- Skips processing in data-only dump mode as constraints are schema objects
- Index-backed constraints (PK/UNIQUE) require careful coordination with associated indexes
- Foreign key constraints on partitioned tables don't use ONLY keyword (inherit to partitions)
- Check constraints are only dumped if they're marked as 'separate' and 'local' (not inherited)
- Domain constraints get special comment handling with qualified object names
- Binary upgrade mode requires special handling for object OID preservation
- Keeps synchronization with dumpIndex for shared index properties like clustering and replica identity
- All constraints are dumped in SECTION_POST_DATA to ensure proper restoration order