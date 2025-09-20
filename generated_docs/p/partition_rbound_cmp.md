# partition_rbound_cmp

## Location
[src/backend/partitioning/partbounds.c:3488-3555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3488-L3555)

## Overview
Compares two range partition bounds to determine their relative ordering, serving as the fundamental comparison function for range partition bound operations.

## Definition

```c
enum elements
		 * compare the same way as the values they represent.
		 */
		if (kind1[i] < kind2[i])
			return -colnum;
```
## Detailed Description
The  function performs a comprehensive comparison between two range partition bounds, taking into account not only the data values but also special boundary conditions (MINVALUE/MAXVALUE) and whether the bounds are upper or lower bounds.

The comparison algorithm operates as follows:
1. **Column-by-column comparison**: Iterates through each partitioning column in order
2. **Special boundary handling**: MINVALUE/MAXVALUE are compared based on their enum values without invoking comparison functions
3. **Data value comparison**: For concrete values, uses the appropriate comparison function with collation
4. **Boundary type resolution**: When values are equal, upper bounds are considered smaller than lower bounds

The function returns:
- **0** if bounds are equal
- **Negative value** if first bound is less than second bound  
- **Positive value** if first bound is greater than second bound
- **Absolute value** indicates the 1-based column number where the first difference was found

This ordering is crucial for RelationBuildPartitionDesc() which relies on the fact that upper bounds sort before lower bounds when values are equal, allowing it to store only upper bounds for contiguous partition boundaries.

## Parameters / Member Variables
- : Number of partitioning attributes to compare
- : Array of comparison functions for each partitioning column
- : Array of collation OIDs for each partitioning column
- : Array of datum values for the first bound
- : Array of datum kinds (VALUE/MINVALUE/MAXVALUE) for the first bound
- : Boolean indicating if first bound is a lower bound
- : PartitionRangeBound structure containing the second bound's data

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (comparison function invocation)
  - [DatumGetInt32](../D/DatumGetInt32.md) (result extraction)
  - PARTITION_RANGE_DATUM_VALUE (constant)
- Called from (representative examples):
  - compare_range_bounds
  - [add_merged_range_bounds](../a/add_merged_range_bounds.md)
  - [check_new_partition_bound](../c/check_new_partition_bound.md)
  - [partition_range_bsearch](partition_range_bsearch.md)

## Notes and Other Information
- This is a static function, only accessible within the partbounds.c file
- The function handles unbounded values (MINVALUE/MAXVALUE) specially without invoking comparison procedures
- [PartitionRangeDatumKind](../P/PartitionRangeDatumKind.md) enum values are designed to compare correctly when cast to integers
- The boundary type comparison (upper vs lower) is essential for maintaining the partition descriptor's invariant that only upper bounds are stored for contiguous partitions
- Return value encoding allows callers to identify both the ordering and the specific column where the difference occurred