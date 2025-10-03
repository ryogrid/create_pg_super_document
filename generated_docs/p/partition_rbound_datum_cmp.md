# partition_rbound_datum_cmp

## Location
[src/backend/partitioning/partbounds.c:3556-3586](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3556-L3586)

## Overview
Compares a range partition bound against a tuple's partitioning key values to determine the relative ordering for tuple routing and partition pruning operations.

## Definition

```c
int32
partition_rbound_datum_cmp(FmgrInfo *partsupfunc, Oid *partcollation,
						   Datum *rb_datums, PartitionRangeDatumKind *rb_kind,
						   Datum *tuple_datums, int n_tuple_datums)
```
## Detailed Description
The  function performs a comparison between a range partition boundary and the partitioning key values from a tuple. This is a critical function used for tuple routing (determining which partition a tuple belongs to) and partition pruning (eliminating partitions that cannot contain matching tuples).

The comparison algorithm:
1. **Special boundary handling**: Immediately returns -1 for MINVALUE bounds and +1 for MAXVALUE bounds, since these represent infinite boundaries
2. **Column-by-column comparison**: For concrete values, compares each partitioning column using the appropriate comparison function with collation
3. **Early termination**: Stops at the first non-equal column and returns the comparison result

This function differs from  in that it compares a bound against tuple data rather than another bound, and it doesn't need to handle lower/upper bound distinctions since tuples don't have such concepts.

The function returns:
- **Negative value** if the range bound is less than the tuple values
- **0** if they are equal
- **Positive value** if the range bound is greater than the tuple values

## Parameters / Member Variables
- `*partsupfunc`: Array of comparison functions for each partitioning column
- `*partcollation`: Array of collation OIDs for each partitioning column
- `*rb_datums`: Array of datum values from the range bound
- `*rb_kind`: Array of datum kinds (VALUE/MINVALUE/MAXVALUE) from the range bound
- `*tuple_datums`: Array of datum values from the tuple's partitioning key
- `n_tuple_datums`: Number of partitioning attributes in the tuple
## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (comparison function invocation)
  - [DatumGetInt32](../D/DatumGetInt32.md) (result extraction)
  - PARTITION_RANGE_DATUM_MINVALUE / PARTITION_RANGE_DATUM_MAXVALUE (constants)
- Called from (representative examples):
  - [get_partition_for_tuple](../g/get_partition_for_tuple.md) (tuple routing)
  - [partition_range_datum_bsearch](partition_range_datum_bsearch.md) (binary search operations)
  - [get_matching_range_bounds](../g/get_matching_range_bounds.md) (partition pruning)

## Notes and Other Information
- This is a public function, accessible from other modules as declared in partbounds.h
- Critical for runtime tuple routing performance in partitioned tables
- MINVALUE boundaries are always considered smaller than any tuple data
- MAXVALUE boundaries are always considered larger than any tuple data
- Used extensively in both executor (for INSERT/UPDATE routing) and optimizer (for partition pruning) components
- The function assumes tuple_datums contains exactly n_tuple_datums valid partitioning key values

## Simplified Source

```c
int32
partition_rbound_datum_cmp(FmgrInfo *partsupfunc, Oid *partcollation,
                          Datum *rb_datums, PartitionRangeDatumKind *rb_kind,
                          Datum *tuple_datums, int n_tuple_datums)
{
    int32 cmpval = -1;

    // Compare each partitioning column
    for (int i = 0; i < n_tuple_datums; i++) {
        // Handle special boundary values
        if (rb_kind[i] == PARTITION_RANGE_DATUM_MINVALUE)
            return -1;  // MINVALUE is always smaller
        else if (rb_kind[i] == PARTITION_RANGE_DATUM_MAXVALUE)
            return 1;   // MAXVALUE is always larger

        // Compare actual values using appropriate comparison function
        cmpval = DatumGetInt32(FunctionCall2Coll(&partsupfunc[i],
                                                partcollation[i],
                                                rb_datums[i],
                                                tuple_datums[i]));

        // Return on first non-equal comparison
        if (cmpval != 0)
            break;
    }

    return cmpval;
}
```