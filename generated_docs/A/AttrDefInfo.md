# AttrDefInfo

## Location
src/bin/pg_dump/pg_dump.h: 395 - 396

## Overview
AttrDefInfo represents column default value expressions in PostgreSQL's pg_dump utility, storing information about DEFAULT clauses that need to be dumped and restored for table columns.

## Definition


## Detailed Description
AttrDefInfo encapsulates information about column default value expressions stored in the pg_attrdef system catalog. Each instance represents a DEFAULT clause for a specific table column that needs to be included in database dumps. The structure contains the decompiled DEFAULT expression and metadata about how it should be dumped - either inline within the CREATE TABLE statement or as a separate ALTER TABLE ... ALTER COLUMN ... SET DEFAULT statement. This separation is necessary for cases where the default expression has dependencies that must be resolved after the table is created, such as references to user-defined functions or types.

## Parameters / Member Variables
- : Base DumpableObject containing common dump metadata (object ID, dependencies, etc.); note that dobj.name contains the table name, not column name
- : Pointer to the TableInfo structure representing the table that contains this column
- : Column number (1-based) within the table for which this default applies
- : Decompiled DEFAULT expression as a string, obtained from pg_get_expr()
- : Boolean flag indicating whether the default must be dumped as a separate ALTER TABLE statement rather than inline in CREATE TABLE

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - TableInfo
  - pg_get_expr
  - AssignDumpId
  - addObjectDependency
- Called from (representative examples):
  - getTableAttrs (src/bin/pg_dump/pg_dump.c:9083)
  - dumpAttrDef (src/bin/pg_dump/pg_dump.c:16876)
  - flagInhAttrs (src/bin/pg_dump/common.c:563)

## Notes and Other Information
- AttrDefInfo objects are created during table attribute analysis in getTableAttrs() function
- Default expressions are retrieved using pg_get_expr() to get the decompiled, readable form
- The separate flag is set based on dependency analysis - defaults referencing user-defined objects require separate dumping
- Column numbers are 1-based to match PostgreSQL's internal numbering system
- Dropped columns are excluded from default processing
- The structure is linked to its parent table through the adtable pointer for easy navigation
- Dependencies are automatically added to ensure proper restoration order
- Used in conjunction with TableInfo's attrdefs array to maintain column-to-default mappings
- Essential for preserving column default behavior during database restoration
- Handles complex DEFAULT expressions including function calls, sequences, and computed values