# get_partition_strategy

## Location
src/include/utils/partcache.h: 59 - 64

## Overview
Returns the partitioning strategy used by a partitioned table (e.g., range, hash, list partitioning).

## Definition

```c
static inline int
get_partition_strategy(PartitionKey key)
```
## Detailed Description
This inline function provides access to the partitioning strategy field of a PartitionKey structure. The partitioning strategy determines how data is distributed across partitions:
- PARTITION_STRATEGY_RANGE for range partitioning
- PARTITION_STRATEGY_HASH for hash partitioning  
- PARTITION_STRATEGY_LIST for list partitioning

The function serves as an accessor method to encapsulate access to the PartitionKey internal structure, allowing code to retrieve the strategy without directly accessing the structure members.

## Parameters / Member Variables
- `key`: A PartitionKey structure containing partitioning metadata for a partitioned table

## Dependencies
- Functions called/Symbols referenced:
  - PartitionKey (struct type)
- Called from (representative examples):
  - transformPartitionBound

## Notes and Other Information
- This is a static inline function defined in partcache.h for efficient access
- The function returns the strategy field directly from the PartitionKey structure
- Used primarily during partition bound validation and constraint processing
- Located at src/include/utils/partcache.h:55-62