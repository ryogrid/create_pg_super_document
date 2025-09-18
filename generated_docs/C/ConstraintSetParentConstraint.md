# ConstraintSetParentConstraint

## Location
src/backend/catalog/pg_constraint.c: 824 - 896

## Overview
Establishes or removes the inheritance relationship between a partition's constraint and its parent table's constraint, managing dependency tracking for constraint inheritance.

## Definition
void ConstraintSetParentConstraint(Oid childConstrId, Oid parentConstrId, Oid childTableId)

## Detailed Description
ConstraintSetParentConstraint manages the parent-child relationship between constraints in table partitioning scenarios. The function operates in two modes:

1. **Setting parent relationship**: When parentConstrId is valid, it marks the child constraint as inherited (conislocal=false), increments the inheritance count, and establishes dependency relationships to prevent independent deletion of the child constraint.

2. **Removing parent relationship**: When parentConstrId is InvalidOid, it reverses the inheritance by making the constraint local again (conislocal=true), clearing the parent reference, and removing the associated dependency records.

The function ensures proper constraint inheritance semantics by maintaining dependency relationships using DEPENDENCY_PARTITION_PRI (to parent constraint) and DEPENDENCY_PARTITION_SEC (to child table), which prevent orphaned constraints during partition operations.

## Parameters / Member Variables
- : The OID of the child constraint to be linked or unlinked
- : The OID of the parent constraint, or InvalidOid to remove inheritance
- : The OID of the child table that owns the child constraint

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - [heap_copytuple](../h/heap_copytuple.md)
  - GETSTRUCT
  - OidIsValid
  - Assert
  - ereport
  - [CatalogTupleUpdate](CatalogTupleUpdate.md)
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - table_close
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md) (indexcmds.c:1417)
  - [tryAttachPartitionForeignKey](../t/tryAttachPartitionForeignKey.md) (tablecmds.c:11188)
  - [AttachPartitionEnsureIndexes](../A/AttachPartitionEnsureIndexes.md) (tablecmds.c:18939)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md) (tablecmds.c:19400, 19515, 19556)
  - [ATExecAttachPartitionIdx](../A/ATExecAttachPartitionIdx.md) (tablecmds.c:19983)

## Notes and Other Information
- Prevents setting a parent for constraints that already have one (assertion-based check)
- Protects against inheritance count overflow with error reporting
- Uses dual dependency types: PARTITION_PRI (constraint-to-constraint) and PARTITION_SEC (constraint-to-table)
- The dependency system prevents independent deletion of child constraints when inheritance is active  
- Critical component of PostgreSQL's table partitioning constraint inheritance mechanism
- Maintains constraint inheritance counts (coninhcount) for proper inheritance tracking
- Ensures constraints properly reflect their local vs inherited status through conislocal flag