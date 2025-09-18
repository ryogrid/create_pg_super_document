# get_rel_relispartition

## Location
[src/backend/utils/cache/lsyscache.c:2027-2053](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2027-L2053)

## Overview
Returns the relispartition flag associated with a given relation, indicating whether the relation is a partition of a partitioned table.

## Definition


## Detailed Description
This function retrieves the relispartition boolean flag for a specified relation from the system catalog. The relispartition field indicates whether a relation is a partition of a partitioned table in PostgreSQL's table partitioning feature. This information is crucial for determining partition relationships and handling partition-specific operations.

The function performs a system cache lookup on the pg_class catalog using the relation OID and extracts the relispartition field. When a table is created as a partition of a partitioned table, this flag is set to true, allowing PostgreSQL to differentiate between regular tables and partition tables.

## Parameters / Member Variables
- : The OID of the relation to check for partition status

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_class (pg_class catalog structure)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)

- Called from (representative examples):
  - index_concurrently_swap
  - [filter_partitions](../f/filter_partitions.md)
  - [get_rel_sync_entry](get_rel_sync_entry.md)
  - [check_rel_can_be_partition](../c/check_rel_can_be_partition.md)
  - [get_partition_qual_relid](get_partition_qual_relid.md)

## Notes and Other Information
- Returns false if the relation does not exist
- Essential for partition management and partitioning operations
- Used in logical replication to handle partition-specific logic
- Part of PostgreSQL's table partitioning infrastructure introduced in version 10
- Helps distinguish between partitioned tables (parents) and partitions (children)
- Critical for partition pruning and constraint exclusion optimizations
- Located in src/backend/utils/cache/lsyscache.c:2027-2053