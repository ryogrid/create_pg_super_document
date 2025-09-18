# ATExecAlterColumnType

## Location
src/backend/commands/tablecmds.c: 13146 - 13462

## Overview
ATExecAlterColumnType executes the ALTER COLUMN .. SET DATA TYPE command, handling the complex process of changing a column's data type while managing dependencies, defaults, and constraints.

## Definition


## Detailed Description
This function implements the core logic for changing a column's data type in PostgreSQL tables. It performs comprehensive validation and dependency management:

1. **Missing Value Management**: Clears missing values when table rewriting is required
2. **Column Validation**: Verifies the target column exists and prevents multiple type changes
3. **Type Coercion**: Validates that existing default expressions can be coerced to the new type
4. **Dependency Tracking**: Uses RememberAllDependentForRebuilding to record objects that need rebuilding
5. **Catalog Updates**: Updates pg_attribute with new type information including type OID, typmod, collation, and storage parameters
6. **Default Expression Handling**: Removes and recreates default expressions with proper type coercion
7. **Statistics Cleanup**: Removes obsolete statistics entries for the column

The function ensures data integrity by carefully managing all dependent objects and maintaining referential consistency throughout the type change operation.

## Parameters / Member Variables
- : AlteredTableInfo structure containing table modification context and rewrite information
- : Relation being modified
- : AlterTableCmd containing the column name and new type definition
- : Lock mode for the operation

## Dependencies
- Functions called/Symbols referenced:
  - RememberAllDependentForRebuilding
  - SearchSysCacheCopyAttName
  - typenameType
  - GetColumnDefCollation
  - build_column_default
  - strip_implicit_coercions
  - coerce_to_target_type
  - add_column_datatype_dependency
  - add_column_collation_dependency
  - RemoveStatistics
  - GetAttrDefaultOid
  - deleteDependencyRecordsFor
  - RemoveAttrDefault
  - StoreAttrDefault
  - CatalogTupleUpdate
  - heap_freetuple
  - relation_close
- Called from (representative examples):
  - ATExecCmd (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Handles both regular and generated column defaults differently
- Manages missing value arrays when changing types without table rewrite
- Prevents multiple ALTER TYPE operations on the same column in one transaction
- Updates compression method to invalid when changing types
- Maintains array dimension information from the type specification
- Uses RESTRICT mode when removing old defaults for safety