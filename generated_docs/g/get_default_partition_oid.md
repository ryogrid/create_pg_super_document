# get_default_partition_oid

## Location
[src/backend/catalog/partition.c:315-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/partition.c#L315-L339)

## Overview
Retrieves the OID of the default partition for a given partitioned table by looking up the partdefid field in the pg_partitioned_table system catalog.

## Definition
```c
Oid get_default_partition_oid(Oid parentId)
```

## Detailed Description
This function provides a direct way to find the default partition OID for a partitioned table. It performs a system cache lookup on the pg_partitioned_table catalog using the parent table's OID. If the partitioned table exists and has a default partition configured, it returns the OID stored in the partdefid field. Otherwise, it returns InvalidOid.

The function is a simpler alternative to get_default_oid_from_partdesc() when you only need the default partition OID and don't require the full partition descriptor structure.

## Parameters / Member Variables
- `parentId`: OID of the partitioned parent table to look up

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (via PARTRELID cache)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - GETSTRUCT (macro)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_partitioned_table (struct type)
- Called from (representative examples):
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md)
  - [RelationBuildPartitionDesc](../R/RelationBuildPartitionDesc.md)

## Notes and Other Information
- Returns InvalidOid if the parentId doesn't correspond to a partitioned table or if no default partition is configured
- The function suggests using get_default_oid_from_partdesc() where possible for efficiency, particularly when working with partition descriptors
- Uses the PARTRELID system cache for fast lookup of pg_partitioned_table entries
- Properly manages system cache resources by releasing the tuple after use