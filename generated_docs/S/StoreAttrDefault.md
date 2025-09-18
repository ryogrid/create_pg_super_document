# StoreAttrDefault

## Location
src/backend/catalog/pg_attrdef.c: 46 - 218

## Overview
StoreAttrDefault stores a default expression for a specified column in a PostgreSQL relation, creating entries in the pg_attrdef catalog and updating the corresponding pg_attribute entry to mark that a default exists.

## Definition


## Detailed Description
This function creates a new pg_attrdef tuple to store a default expression for a column. It performs several key operations: converts the expression node to string format for storage, creates a new OID for the default entry, inserts the tuple into the pg_attrdef catalog, updates the pg_attribute entry to set atthasdef to true, and establishes proper dependency relationships. The function also handles special logic for missing values when adding new columns to existing tables, though this code path is currently unused in core PostgreSQL. The function ensures data consistency by acquiring appropriate locks and maintaining referential integrity through the dependency system.

## Parameters / Member Variables
- : The relation (table) containing the column for which the default is being stored
- : The attribute number (column number) within the relation
- : The default expression node to be stored
- : Boolean indicating whether this is an internal default (affects hook invocation)
- : Boolean indicating if this is for a new column (affects missing value handling)

## Dependencies
- Functions called/Symbols referenced:
  - [nodeToString](../n/nodeToString.md): Converts expression node to string representation
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md): Generates new OID for the pg_attrdef entry
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates heap tuple from values array
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md): Inserts tuple into catalog
  - [heap_freetuple](../h/heap_freetuple.md): Frees heap tuple memory
  - SearchSysCacheCopy2: Searches system cache for attribute entry
  - [recordDependencyOn](../r/recordDependencyOn.md): Records dependency between default and column
  - [recordDependencyOnSingleRelExpr](../r/recordDependencyOnSingleRelExpr.md): Records dependencies on expression objects
  - InvokeObjectPostCreateHookArg: Invokes post-creation hooks

- Called from (representative examples):
  - [StoreConstraints](StoreConstraints.md): When storing table constraints during creation
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md): When adding new constraints to relations
  - [ATExecCookedColumnDefault](../A/ATExecCookedColumnDefault.md): During ALTER TABLE column default operations
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md): When changing column types that affect defaults

## Notes and Other Information
The function includes legacy code for handling missing values when adding columns (add_column_mode), but this functionality is currently unused in core PostgreSQL as noted in the comments. The function carefully manages memory allocation and deallocation, freeing temporary structures like the stringified expression and heap tuples. It establishes proper dependency relationships to ensure cascading deletion behavior when columns or tables are dropped. For generated columns, it creates internal dependencies to prevent separate deletion of the default expression.