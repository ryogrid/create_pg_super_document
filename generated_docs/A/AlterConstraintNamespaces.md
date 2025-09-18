# AlterConstraintNamespaces

## Location
src/backend/catalog/pg_constraint.c: 755 - 823

## Overview
Moves all constraints belonging to a specified object (relation or domain type) from one namespace to another during namespace change operations.

## Definition
void AlterConstraintNamespaces(Oid ownerId, Oid oldNspId, Oid newNspId, bool isType, ObjectAddresses *objsMoved)

## Detailed Description
AlterConstraintNamespaces is responsible for updating the namespace of constraints when their owning objects are moved between schemas. The function performs a comprehensive scan of the pg_constraint catalog to locate all constraints associated with the specified owner object and updates their namespace accordingly.

The function handles both relation constraints and domain type constraints by using conditional logic based on the isType parameter. It scans using a composite key that searches either conrelid or contypid depending on the object type. The function ensures constraints are only moved if they haven't already been processed (tracked via objsMoved) and only if a namespace change is actually needed.

## Parameters / Member Variables
- : The OID of the relation or type that owns the constraints to be moved
- : The OID of the current namespace the constraints belong to
- : The OID of the target namespace to move constraints to  
- : Boolean flag indicating whether the owner is a domain type (true) or relation (false)
- : ObjectAddresses structure tracking already-processed objects to prevent duplicates

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - HeapTupleIsValid
  - GETSTRUCT
  - ObjectAddressSet
  - object_address_present
  - heap_copytuple
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - add_exact_object_address
  - systable_endscan
  - table_close
- Called from (representative examples):
  - AlterTableNamespaceInternal (tablecmds.c:17303)
  - AlterTypeNamespaceInternal (typecmds.c:4261, 4268)

## Notes and Other Information
- Uses ConstraintRelidTypidNameIndexId index for efficient constraint lookup
- Only updates constraints that actually belong to the old namespace to avoid unnecessary work
- Constraints don't maintain their own namespace dependencies, so changeDependencyFor() is not needed
- Post-alter hooks are invoked for all processed constraints regardless of whether they were updated
- The objsMoved parameter prevents duplicate processing when multiple objects reference the same constraints
- Designed to work seamlessly with schema alteration operations for both tables and domain types