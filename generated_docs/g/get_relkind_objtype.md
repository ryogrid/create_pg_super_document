# get_relkind_objtype

## Location
src/backend/catalog/objectaddress.c: 6098 - 6122

## Overview
Maps PostgreSQL relation kinds (relkind) to corresponding ObjectType enumeration values, providing a standardized way to identify database object types for access control and error messaging.

## Definition
ObjectType get_relkind_objtype(char relkind)

## Detailed Description
This function serves as a translation layer between PostgreSQL's internal relation kind identifiers (stored as single characters in pg_class.relkind) and the ObjectType enumeration used throughout the system for object identification and access control checks. The function implements a defensive design philosophy by defaulting to OBJECT_TABLE for any unexpected relkind values rather than raising errors.

This approach is particularly important for ACL (Access Control List) error message generation, where producing a generic "table" message is preferable to system failure. The function handles all major PostgreSQL relation types including regular tables, partitioned tables, indexes, sequences, views, materialized views, foreign tables, and TOAST tables.

## Parameters / Member Variables
- relkind: A single character representing the relation kind as stored in pg_class.relkind (e.g., 'r' for regular table, 'i' for index, 'S' for sequence, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_RELATION: Constant for regular table relation kind
  - RELKIND_PARTITIONED_TABLE: Constant for partitioned table relation kind
  - RELKIND_INDEX: Constant for index relation kind
  - RELKIND_PARTITIONED_INDEX: Constant for partitioned index relation kind
  - RELKIND_SEQUENCE: Constant for sequence relation kind
  - RELKIND_VIEW: Constant for view relation kind
  - RELKIND_MATVIEW: Constant for materialized view relation kind
  - RELKIND_FOREIGN_TABLE: Constant for foreign table relation kind
  - RELKIND_TOASTVALUE: Constant for TOAST table relation kind
  - OBJECT_TABLE: ObjectType enumeration value for tables
  - OBJECT_INDEX: ObjectType enumeration value for indexes
  - OBJECT_SEQUENCE: ObjectType enumeration value for sequences
  - OBJECT_VIEW: ObjectType enumeration value for views
  - OBJECT_MATVIEW: ObjectType enumeration value for materialized views
  - OBJECT_FOREIGN_TABLE: ObjectType enumeration value for foreign tables

- Called from (representative examples):
  - RangeVarGetAndCheckCreationNamespace: Namespace creation checks
  - get_object_type: General object type identification
  - ExecCheckPermissions: Permission checking during query execution
  - ATSimplePermissions: ALTER TABLE permission checks
  - CreateTriggerFiringOn: Trigger creation permission validation

## Notes and Other Information
- Implements defensive programming by defaulting to OBJECT_TABLE for unknown relkind values
- Primarily used for generating user-friendly error messages in ACL checks
- Both regular and partitioned tables/indexes map to the same object types
- TOAST tables are treated as regular tables for permission purposes
- Critical component in PostgreSQL's access control and object identification infrastructure
- The function never raises errors, making it safe for use in error handling paths