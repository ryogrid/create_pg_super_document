# update_default_partition_oid

## Location
[src/backend/catalog/partition.c:340-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/partition.c#L340-L369)

## Overview
Updates the default partition OID for a partitioned table by modifying the partdefid field in the pg_partitioned_table system catalog.

## Definition
```c
void update_default_partition_oid(Oid parentId, Oid defaultPartId)
```

## Detailed Description
This function modifies the pg_partitioned_table catalog to set or update the default partition OID for a specified partitioned table. It performs a direct catalog update operation with proper locking and error handling.

The function operates by:
1. Opening the pg_partitioned_table relation with RowExclusiveLock
2. Looking up the existing tuple for the parent table using system cache
3. Modifying the partdefid field in the tuple structure
4. Updating the catalog using CatalogTupleUpdate
5. Cleaning up resources including freeing the tuple and closing the relation

## Parameters / Member Variables
- `parentId`: OID of the partitioned parent table to update
- `defaultPartId`: OID of the default partition to set (can be InvalidOid to clear)

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - SearchSysCacheCopy1 
  - HeapTupleIsValid
  - GETSTRUCT (macro)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - table_close
  - Form_pg_partitioned_table (struct type)
- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)
  - [StorePartitionBound](../S/StorePartitionBound.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)

## Notes and Other Information
- Uses RowExclusiveLock to ensure exclusive access during the catalog update
- Throws an ERROR if the partitioned table lookup fails, indicating a serious catalog inconsistency
- Uses SearchSysCacheCopy1 to get a modifiable copy of the tuple rather than the cached version
- Properly manages memory by calling heap_freetuple() on the copied tuple
- The defaultPartId can be set to InvalidOid to indicate no default partition exists