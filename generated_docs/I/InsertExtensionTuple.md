# InsertExtensionTuple

## Location
src/backend/commands/extension.c: 1866 - 1953

## Overview
InsertExtensionTuple creates a new pg_extension catalog tuple and establishes all necessary dependency relationships for the extension, including owner, schema, and prerequisite extensions.

## Definition


## Detailed Description
This function performs the core catalog operations for extension registration. It creates a new tuple in the pg_extension system catalog with all the extension metadata, generates a unique OID for the extension, and establishes dependency relationships. The function handles both required and optional extension configuration arrays (extConfig and extCondition), records dependencies on the extension owner, target schema, and all prerequisite extensions, and invokes post-creation hooks. It's specifically designed to be usable by pg_upgrade, which needs to create extension entries without running installation scripts.

## Parameters / Member Variables
- : Name of the extension to register
- : OID of the user who owns the extension
- : OID of the schema where the extension is installed
- : Boolean flag indicating if the extension can be moved between schemas
- : Version string of the extension being installed
- : Configuration array (tables/views) or NULL pointer as Datum
- : Condition array (WHERE clauses for config tables) or NULL pointer as Datum  
- : List of OIDs of extensions that this extension depends on

## Dependencies
- Functions called/Symbols referenced:
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - DirectFunctionCall1
  - namein
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md)
  - [new_object_addresses](../n/new_object_addresses.md)
  - ObjectAddressSet
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - InvokeObjectPostCreateHook
- Called from (representative examples):
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)
  - [binary_upgrade_create_empty_extension](../b/binary_upgrade_create_empty_extension.md)

## Notes and Other Information
- This function is exported specifically for pg_upgrade support, allowing extension registration without script execution
- Uses RowExclusiveLock on pg_extension catalog during tuple insertion
- Handles nullable extConfig and extCondition fields properly using PointerGetDatum(NULL) checks
- Records DEPENDENCY_NORMAL relationships to owner, schema, and prerequisite extensions
- Implements proper memory management with heap_freetuple and free_object_addresses cleanup
- Invokes object creation hooks for extension registration events
- Returns ObjectAddress for the newly created extension for further processing