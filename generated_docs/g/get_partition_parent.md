# get_partition_parent

## Location
src/backend/catalog/partition.c: 53 - 84

## Overview
Obtains the direct parent of a given partition relation by scanning the pg_inherits catalog table.

## Definition
```c
Oid get_partition_parent(Oid relid, bool even_if_detached)
```

## Detailed Description
This function retrieves the inheritance parent of a partition by querying the pg_inherits system catalog. It opens the InheritsRelationId catalog table with AccessShareLock and delegates the actual lookup work to `get_partition_parent_worker`. The function includes error handling for cases where no parent is found and special logic for partitions that are in the process of being detached.

The function assumes that the relation whose OID is passed as an argument will have precisely one parent, so it should only be called when it is known that the relation is a partition.

## Parameters / Member Variables
- `relid`: OID of the partition relation whose parent is to be found
- `even_if_detached`: If true, allows returning parent even if the partition is being detached; if false, throws an error for detaching partitions

## Dependencies
- Functions called/Symbols referenced:
  - table_open (to access InheritsRelationId catalog)
  - [get_partition_parent_worker](get_partition_parent_worker.md) (performs the actual parent lookup)
  - table_close (to release catalog lock)
  - OidIsValid (to validate the result)
  - elog (for error reporting)

- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)
  - [index_get_partition](../i/index_get_partition.md)
  - [RangeVarCallbackForDropRelation](../R/RangeVarCallbackForDropRelation.md)
  - [ATExecDropNotNull](../A/ATExecDropNotNull.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)
  - [ATExecAttachPartitionIdx](../A/ATExecAttachPartitionIdx.md)
  - [validatePartitionedIndex](../v/validatePartitionedIndex.md)
  - [renametrig](../r/renametrig.md)
  - [generate_partition_qual](generate_partition_qual.md)

## Notes and Other Information
- The function throws an error if no parent tuple is found for the given relation
- If the partition is being detached and `even_if_detached` is false, it throws an error
- Uses AccessShareLock for safe concurrent access to the pg_inherits catalog
- Located at src/backend/catalog/partition.c:53-84