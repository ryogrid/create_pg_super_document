# get_hash_partition_greatest_modulus

## Location
[src/backend/partitioning/partbounds.c:3414-3427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3414-L3427)

## Overview
Returns the greatest modulus value among all hash partitions in a hash-partitioned table, which represents the total number of hash partitions.

## Definition

```c
int
get_hash_partition_greatest_modulus(PartitionBoundInfo bound)
```
## Detailed Description
The  function is a utility function that extracts the greatest modulus value from hash partition bounds. In PostgreSQL's hash partitioning scheme, each partition is defined by a modulus and remainder pair. The greatest modulus among all partitions represents the total number of hash partitions in the partitioned table.

This function was part of the core partitioning logic but is no longer actively used in PostgreSQL's internal code. However, it is retained for backward compatibility to support external modules that may depend on this interface.

The function simply returns the  field from the PartitionBoundInfo structure, which for hash partitions represents the total number of partitions (i.e., the greatest modulus).

## Parameters / Member Variables
- `bound`: Partition bound information structure containing hash partition metadata
## Dependencies
- Functions called/Symbols referenced:
  - PARTITION_STRATEGY_HASH (constant for validation)
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md) (structure type)
- Called from (representative examples):
  - External modules (maintained for compatibility)

## Notes and Other Information
- This function includes an assertion to ensure the bound is for hash partitioning strategy
- The function is deprecated in core PostgreSQL code but maintained for external module compatibility
- For hash partitions,  directly corresponds to the greatest modulus value
- The return value represents the total number of hash partitions that should exist for complete coverage