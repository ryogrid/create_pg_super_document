# CreateTransform

## Location
[src/backend/commands/functioncmds.c:1814-2018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L1814-L2018)

## Overview
Implements the CREATE TRANSFORM command to define data conversion functions between SQL data types and procedural language representations.

## Definition


## Detailed Description
CreateTransform processes CREATE TRANSFORM statements to establish bidirectional data conversion mechanisms between PostgreSQL's SQL data types and procedural language-specific data representations. The function supports creating or replacing transform entries in the pg_transform system catalog.

Key operations include:
1. **Type validation** - Ensures the target type is valid (not pseudo-type or domain)
2. **Permission checks** - Validates ownership/usage rights on type, language, and functions
3. **Function validation** - Verifies transform functions meet strict requirements via check_transform_function
4. **Transform function requirements**:
   - FROM SQL: Must return 'internal' type to pass data to procedural language
   - TO SQL: Must return the transform data type to convert back to SQL
5. **Catalog management** - Handles both new transform creation and replacement of existing transforms
6. **Dependency management** - Records dependencies on type, language, and transform functions
7. **Extension integration** - Properly handles extension membership for transforms

The function supports optional FROM SQL and TO SQL transform functions, allowing unidirectional or bidirectional conversions as needed.

## Parameters / Member Variables
- : CreateTransformStmt structure containing type name, language name, optional FROM SQL function, optional TO SQL function, and replace flag

## Dependencies
- Functions called/Symbols referenced:
  - [typenameTypeId](../t/typenameTypeId.md)
  - [get_typtype](../g/get_typtype.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [get_language_oid](../g/get_language_oid.md)
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)/SearchSysCache2
  - [check_transform_function](../c/check_transform_function.md)
  - table_open
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](CatalogTupleUpdate.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](CatalogTupleInsert.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility.c:1740)

## Notes and Other Information
- Supports the REPLACE option to update existing transforms without dropping and recreating
- Enforces strict ownership requirements: must own the type and transform functions
- Requires USAGE privilege on type and language, EXECUTE privilege on transform functions
- Transform functions must meet specific signature requirements validated by check_transform_function
- FROM SQL functions convert from SQL type to procedural language representation (return type: internal)
- TO SQL functions convert from procedural language back to SQL type (return type: target type)
- Manages dependencies carefully to ensure proper cleanup when objects are dropped
- Integrates with extension system for proper packaging and dependency management