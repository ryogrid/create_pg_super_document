# ATExecAlterColumnType

## Location
[src/backend/commands/tablecmds.c:13146-13462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L13146-L13462)

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
  - [RememberAllDependentForRebuilding](../R/RememberAllDependentForRebuilding.md)
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md)
  - [typenameType](../t/typenameType.md)
  - [GetColumnDefCollation](../G/GetColumnDefCollation.md)
  - [build_column_default](../b/build_column_default.md)
  - [strip_implicit_coercions](../s/strip_implicit_coercions.md)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - [add_column_datatype_dependency](../a/add_column_datatype_dependency.md)
  - [add_column_collation_dependency](../a/add_column_collation_dependency.md)
  - [RemoveStatistics](../R/RemoveStatistics.md)
  - [GetAttrDefaultOid](../G/GetAttrDefaultOid.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)
  - [RemoveAttrDefault](../R/RemoveAttrDefault.md)
  - [StoreAttrDefault](../S/StoreAttrDefault.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Handles both regular and generated column defaults differently
- Manages missing value arrays when changing types without table rewrite
- Prevents multiple ALTER TYPE operations on the same column in one transaction
- Updates compression method to invalid when changing types
- Maintains array dimension information from the type specification
- Uses RESTRICT mode when removing old defaults for safety