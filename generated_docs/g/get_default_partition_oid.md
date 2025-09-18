# get_default_partition_oid

## Location
src/backend/catalog/partition.c: 315 - 339

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
  - SearchSysCache1 (via PARTRELID cache)
  - ObjectIdGetDatum
  - HeapTupleIsValid
  - GETSTRUCT (macro)
  - ReleaseSysCache
  - Form_pg_partitioned_table (struct type)
- Called from (representative examples):
  - heap_drop_with_catalog
  - RelationBuildPartitionDesc

## Notes and Other Information
- Returns InvalidOid if the parentId doesn't correspond to a partitioned table or if no default partition is configured
- The function suggests using get_default_oid_from_partdesc() where possible for efficiency, particularly when working with partition descriptors
- Uses the PARTRELID system cache for fast lookup of pg_partitioned_table entries
- Properly manages system cache resources by releasing the tuple after use