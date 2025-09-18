# get_rel_relkind

## Location
src/backend/utils/cache/lsyscache.c: 2003 - 2026

## Overview
Returns the relkind (relation kind) character associated with a given relation, indicating the type of database object (table, index, view, etc.).

## Definition


## Detailed Description
This function retrieves the relation kind (relkind) for a specified relation from the system catalog. The relkind field is a single character that identifies the type of relation object in PostgreSQL's catalogs. Common relkind values include 'r' for regular tables, 'i' for indexes, 'v' for views, 'S' for sequences, 'f' for foreign tables, 'p' for partitioned tables, and others.

The function performs a system cache lookup on the pg_class catalog using the relation OID and extracts the relkind field. This information is critical for determining how to handle different types of database objects in various PostgreSQL operations.

## Parameters / Member Variables
- : The OID of the relation whose kind is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_class (pg_class catalog structure)
  - ObjectIdGetDatum (OID to Datum conversion)

- Called from (representative examples):
  - doDeletion
  - RangeVarGetAndCheckCreationNamespace
  - get_object_type
  - ReindexIndex
  - LockTableCommand
  - RenameRelation
  - CreateTriggerFiringOn
  - ExecCheckPermissions
  - has_sequence_privilege_name_name
  - pg_get_triggerdef_worker

## Notes and Other Information
- Returns '\0' (null character) if the relation does not exist
- Critical for type checking and validation in many PostgreSQL operations
- Common relkind values: 'r' (table), 'i' (index), 'v' (view), 'S' (sequence), 'f' (foreign table), 'p' (partitioned table)
- Used extensively throughout PostgreSQL for relation type discrimination
- Essential for permission checking, DDL operations, and catalog management
- Located in src/backend/utils/cache/lsyscache.c:2003-2026