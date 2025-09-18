# PartitionListValue

## Location
src/backend/partitioning/partbounds.c: 57 - 61

## Overview
PartitionListValue represents one value from a list partition, used during the sorting and processing of partition bounds when reading from the catalog.

## Definition
```c
typedef struct PartitionListValue
{
    int         index;
    Datum       value;
} PartitionListValue;
```

## Detailed Description
PartitionListValue is a structure that encapsulates a single value from a list partition along with its associated partition index. In PostgreSQL's list partitioning scheme, each partition is defined by a specific set of values, and this structure represents one such value during internal processing. It is primarily used when sorting and organizing list partition bounds after reading them from the system catalog.

## Parameters / Member Variables
- `index`: The index or identifier of the partition that contains this value
- `value`: The actual partition value stored as a Datum (PostgreSQL's generic data type)

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL data type)
- Called from (representative examples):
  - [create_list_bounds](../c/create_list_bounds.md) (multiple references)
  - [qsort_partition_list_value_cmp](../q/qsort_partition_list_value_cmp.md)

## Notes and Other Information
This structure is part of the internal machinery for list partitioning in PostgreSQL. The Datum type allows it to store values of any PostgreSQL data type. The structure is used during the sorting process to maintain the association between partition values and their corresponding partition indices.