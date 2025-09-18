# PartitionHashBound

## Location
src/backend/partitioning/partbounds.c: 49 - 54

## Overview
PartitionHashBound represents one bound of a hash partition used during the qsort operation when reading partition bounds from the catalog.

## Definition
```c
typedef struct PartitionHashBound
{
    int         modulus;
    int         remainder;
    int         index;
} PartitionHashBound;
```

## Detailed Description
PartitionHashBound is a structure that encapsulates the essential information needed to define a hash partition boundary. It is used internally by PostgreSQL's partitioning system when sorting and organizing hash partition bounds after reading them from the system catalog. Hash partitioning in PostgreSQL divides data based on hash values of the partition key, and each partition is defined by a modulus and remainder pair that determines which hash values belong to that partition.

## Parameters / Member Variables
- `modulus`: The modulus value used in hash partitioning to determine the total number of partitions
- `remainder`: The remainder value that identifies which specific partition this bound represents
- `index`: The index or identifier of this partition bound within the partition set

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure)
- Called from (representative examples):
  - create_hash_bounds (multiple references)
  - qsort_partition_hbound_cmp

## Notes and Other Information
This structure is specifically used during the sorting process of partition bounds and is part of PostgreSQL's internal partitioning implementation. The modulus and remainder values work together to implement hash partitioning where hash(partition_key) % modulus = remainder determines partition assignment.