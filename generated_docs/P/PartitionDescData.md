# PartitionDescData

## Location
[src/include/partitioning/partdesc.h:29-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/partitioning/partdesc.h#L29-L64)

## Overview
PartitionDescData is a struct that contains comprehensive information about partitions of a partitioned table, including metadata about partition OIDs, bounds, leaf status, and caching fields to optimize partition lookup operations.

## Definition

```c
typedef struct PartitionDescData
{
	int			nparts;			/* Number of partitions */
	bool		detached_exist; /* Are there any detached partitions? */
	Oid		   *oids;			/* Array of 'nparts' elements containing
								 * partition OIDs in order of their bounds */
	bool	   *is_leaf;		/* Array of 'nparts' elements storing whether
								 * the corresponding 'oids' element belongs to
								 * a leaf partition or not */
	PartitionBoundInfo boundinfo;	/* collection of partition bounds */

	/* Caching fields to cache lookups in get_partition_for_tuple() */

	/*
	 * Index into the PartitionBoundInfo's datum array for the last found
	 * partition or -1 if none.
	 */
	int			last_found_datum_index;

	/*
	 * Partition index of the last found partition or -1 if none has been
	 * found yet.
	 */
	int			last_found_part_index;

	/*
	 * For LIST partitioning, this is the number of times in a row that the
	 * datum we're looking for a partition for matches the datum in the
	 * last_found_datum_index index of the boundinfo->datums array.  For RANGE
	 * partitioning, this is the number of times in a row we've found that the
	 * datum we're looking for a partition for falls into the range of the
	 * partition corresponding to the last_found_datum_index index of the
	 * boundinfo->datums array.
	 */
	int			last_found_count;
} PartitionDescData;
```
## Detailed Description
PartitionDescData serves as the central data structure for managing partition metadata in PostgreSQL's partitioned tables. This structure is designed to handle both active and detached partitions, with special consideration for caching to optimize frequent partition lookups.

The structure maintains arrays of partition OIDs and their leaf status, ensuring that partitions are stored in order of their bounds. The detached_exist flag helps the system understand when detached partitions are present, which affects caching behavior since detached partition visibility depends on the snapshot used by each caller.

For performance optimization, the structure includes caching fields specifically designed to speed up get_partition_for_tuple() operations. These fields maintain state about the most recently found partition, allowing the system to take advantage of locality patterns where consecutive tuple lookups often target the same partition.

## Parameters / Member Variables
- : Total number of partitions in the partitioned table
- : Boolean flag indicating whether any detached partitions exist
- : Array containing partition OIDs ordered by their partition bounds
- : Array indicating whether each corresponding partition OID represents a leaf partition (not further partitioned)
- : Collection of partition boundary information used for determining which partition a tuple belongs to
- : Index into PartitionBoundInfo's datum array for the most recently found partition (-1 if none)
- : Partition index of the most recently found partition (-1 if none found yet)
- : Counter for consecutive hits on the same partition, used differently for LIST vs RANGE partitioning to optimize lookup performance

## Dependencies
- Types referenced:
  - [PartitionBoundInfo](PartitionBoundInfo.md) (partition boundary information structure)
  - Oid (PostgreSQL object identifier type)
  - [bool](../b/bool.md) (boolean type)
  - int (integer type)
- Used by:
  - [PartitionDesc](PartitionDesc.md) (typedef pointer to PartitionDescData)
  - [RelationBuildPartitionDesc](../R/RelationBuildPartitionDesc.md) (function that builds partition descriptors)

## Notes and Other Information
- For partitioned tables with detached partitions, PostgreSQL only caches descriptors that include all partitions. When a descriptor without detached partitions is requested, it's created fresh each time to handle snapshot-dependent visibility
- The caching mechanism is specifically optimized for get_partition_for_tuple() operations, which are frequently called during tuple routing in INSERT operations
- The last_found_count field behavior differs between LIST and RANGE partitioning strategies, optimizing for the different access patterns of each partitioning method
- This structure is defined in src/include/partitioning/partdesc.h:29-64 and is central to PostgreSQL's table partitioning infrastructure