# get_partition_natts

## Location
[src/include/utils/partcache.h:65-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/partcache.h#L65-L70)

## Overview
Returns the number of attributes (columns) used in the partition key of a partitioned table.

## Definition
```c
static inline int get_partition_natts(PartitionKey key)
```

## Detailed Description
This inline function provides access to the number of partitioning attributes stored in a PartitionKey structure. The partition attributes count indicates how many columns are used to determine which partition a row belongs to. For example:
- A table partitioned by a single column would return 1
- A table partitioned by multiple columns (composite partitioning) would return the total count

This function serves as an accessor method to encapsulate access to the PartitionKey internal structure, providing a clean interface for retrieving the partition attribute count.

## Parameters / Member Variables
- `key`: A PartitionKey structure containing partitioning metadata for a partitioned table

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionKey](../P/PartitionKey.md) (struct type)
- Called from (representative examples):
  - [has_partition_attrs](../h/has_partition_attrs.md)
  - [ExecBuildSlotPartitionKeyDescription](../E/ExecBuildSlotPartitionKeyDescription.md)
  - [transformPartitionBound](../t/transformPartitionBound.md)

## Notes and Other Information
- This is a static inline function defined in partcache.h for efficient access
- The function returns the partnatts field directly from the PartitionKey structure
- Used extensively in partition processing to iterate over partition columns
- Essential for validating partition bounds and building partition descriptions
- Located at src/include/utils/partcache.h:64-68

## Simplified Source
```c
static inline int
get_partition_natts(PartitionKey key)
{
    // Simple accessor: return number of partition attributes
    return key->partnatts;
}
```