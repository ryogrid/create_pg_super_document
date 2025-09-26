# PartitionBoundInfoData

## Location
[src/include/partitioning/partbounds.h:79-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/partitioning/partbounds.h#L79-L96)

## Overview
PartitionBoundInfoData is a core data structure that encapsulates a set of partition bounds for PostgreSQL's table partitioning feature. It supports hash, list, and range partitioning strategies and is used both for actual partitioned tables and virtual partitioned joinrels within the query planner.

## Definition

```c
typedef struct PartitionBoundInfoData
{
	PartitionStrategy strategy; /* hash, list or range? */
	int			ndatums;		/* Length of the datums[] array */
	Datum	  **datums;
	PartitionRangeDatumKind **kind; /* The kind of each range bound datum;
									 * NULL for hash and list partitioned
									 * tables */
	Bitmapset  *interleaved_parts;	/* Partition indexes of partitions which
									 * may be interleaved. See above. This is
									 * only set for LIST partitioned tables */
	int			nindexes;		/* Length of the indexes[] array */
	int		   *indexes;		/* Partition indexes */
	int			null_index;		/* Index of the null-accepting partition; -1
								 * if there isn't one */
	int			default_index;	/* Index of the default partition; -1 if there
								 * isn't one */
} PartitionBoundInfoData;
```
## Detailed Description
PartitionBoundInfoData serves as the central data structure for managing partition bounds across PostgreSQL's three partitioning strategies:

### Hash Partitioning
- datums contains datum-tuples with 2 datums each: modulus and remainder
- ndatums equals the number of partitions
- nindexes equals the greatest modulus among all partitions
- indexes array is indexed by hash key remainder modulo greatest modulus

### List Partitioning  
- datums contains datum-tuples with key->partnatts datums each
- nindexes equals ndatums
- indexes array stores partition index for each datum
- NULL partition datums are tracked via null_index field, not in datums array
- interleaved_parts tracks partitions that may contain interleaved values

### Range Partitioning
- datums contains datum-tuples with key->partnatts datums each
- ndatums is typically much less than 2 * nparts due to shared bounds
- nindexes equals ndatums + 1 (extra entry for values above last range)
- indexes array contains partition index for upper bounds or -1 for gaps

The datums array is always sorted in increasing order according to the partition key's operator classes and collations.

## Parameters / Member Variables
- : Specifies the partitioning method (hash, list, or range)
- : Number of entries in the datums array
- : Array of datum-tuples containing partition boundary values
- : Array specifying the kind of each range bound datum (NULL for non-range partitioning)
- : Bitmapset tracking potentially interleaved list partitions
- : Number of entries in the indexes array
- : Array mapping boundary positions to partition indexes
- : Index of partition accepting NULL values (-1 if none)
- : Index of default partition (-1 if none)

## Dependencies
- Functions called/Symbols referenced:
  - PartitionStrategy
  - [PartitionRangeDatumKind](PartitionRangeDatumKind.md)
  - Datum
  - [Bitmapset](../B/Bitmapset.md)

- Called from (representative examples):
  - [create_hash_bounds](../c/create_hash_bounds.md)
  - [create_list_bounds](../c/create_list_bounds.md)  
  - [create_range_bounds](../c/create_range_bounds.md)
  - [partition_bounds_copy](../p/partition_bounds_copy.md)
  - [build_merged_partition_bounds](../b/build_merged_partition_bounds.md)
  - [RelOptInfo](../R/RelOptInfo.md) (as part_bounds member)

## Notes and Other Information
- Used in both base relations and join relations, though interleaved_parts is only set for base relations
- The structure efficiently handles the different indexing schemes required by each partitioning strategy
- For list partitioning, interleaved detection helps optimize pruning decisions
- The indexes array design allows for efficient partition lookup across all strategies
- Memory layout is optimized to minimize space usage while supporting fast partition selection