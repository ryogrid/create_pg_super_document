# make_one_partition_rbound

## Location
[src/backend/partitioning/partbounds.c:3428-3487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3428-L3487)

## Overview
Creates a PartitionRangeBound structure from a list of PartitionRangeDatum elements, serving as a factory function for range partition bounds.

## Definition

```c
static PartitionRangeBound *
make_one_partition_rbound(PartitionKey key, int index, List *datums, bool lower)
```
## Detailed Description
The  function is a utility factory function that constructs a PartitionRangeBound structure from raw partition range data. This function centralizes the logic for creating range bounds, which is needed in multiple places throughout the partitioning system.

The function allocates memory for a new PartitionRangeBound structure and populates it with:
- An index identifying the partition
- Arrays for storing the actual datum values and their kinds (types)
- A flag indicating whether this represents a lower or upper bound

For each datum in the input list, the function extracts the datum kind and, if it's a concrete value (not MINVALUE/MAXVALUE), stores the actual data value. The function validates that concrete values are not null, as null values are not permitted in range bounds.

The resulting structure is used throughout the range partitioning system for bound comparisons, partition pruning, and constraint generation.

## Parameters / Member Variables
- : Partition key containing metadata about partitioning columns and their count
- : Integer index identifying which partition this bound belongs to  
- : List of PartitionRangeDatum elements containing the boundary values
- : Boolean flag indicating if this is a lower bound (true) or upper bound (false)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation)
  - lfirst_node (list iteration)
  - castNode (type casting)
  - elog (error reporting)
  - PARTITION_RANGE_DATUM_VALUE (constant)
- Called from (representative examples):
  - compare_range_bounds
  - [create_range_bounds](../c/create_range_bounds.md)  
  - [check_new_partition_bound](../c/check_new_partition_bound.md)

## Notes and Other Information
- This is a static function, only accessible within the partbounds.c file
- The function validates that concrete datum values are not null, throwing an error if null values are encountered
- Memory allocation uses palloc0 to ensure all fields are initialized to zero
- The datums and kind arrays are allocated based on the number of partitioning attributes (key->partnatts)
- Used extensively in range partition bound creation and comparison operations