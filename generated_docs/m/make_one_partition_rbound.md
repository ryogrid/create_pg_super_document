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
- `key`: Partition key containing metadata about partitioning columns and their count
- `index`: Integer index identifying which partition this bound belongs to
- `*datums`: List of PartitionRangeDatum elements containing the boundary values
- `lower`: Boolean flag indicating if this is a lower bound (true) or upper bound (false)
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

## Simplified Source

```c
static PartitionRangeBound *make_one_partition_rbound(PartitionKey key, int index,
                                                     List *datums, bool lower) {
    Assert(datums != NIL);

    // Allocate and initialize the range bound structure
    PartitionRangeBound *bound = (PartitionRangeBound *) palloc0(sizeof(PartitionRangeBound));
    bound->index = index;
    bound->lower = lower;

    // Allocate arrays for datums and their kinds
    bound->datums = (Datum *) palloc0(key->partnatts * sizeof(Datum));
    bound->kind = (PartitionRangeDatumKind *) palloc0(key->partnatts *
                                                     sizeof(PartitionRangeDatumKind));

    // Process each datum in the list
    int i = 0;
    foreach(lc, datums) {
        PartitionRangeDatum *datum = lfirst_node(PartitionRangeDatum, lc);

        // Store the datum kind (MINVALUE, MAXVALUE, or VALUE)
        bound->kind[i] = datum->kind;

        if (datum->kind == PARTITION_RANGE_DATUM_VALUE) {
            // Extract and validate concrete value
            Const *val = castNode(Const, datum->value);
            if (val->constisnull)
                elog(ERROR, "invalid range bound datum");

            bound->datums[i] = val->constvalue;
        }

        i++;
    }

    return bound;
}
```