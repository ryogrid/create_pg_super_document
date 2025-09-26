# PartitionDescData

## Location
src/include/partitioning/partdesc.h: 29 - 64

## Overview
PartitionDescData is a struct that contains comprehensive information about partitions of a partitioned table, including metadata about partition OIDs, bounds, leaf status, and caching fields to optimize partition lookup operations.

## Definition


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
  - PartitionBoundInfo (partition boundary information structure)
  - Oid (PostgreSQL object identifier type)
  - bool (boolean type)
  - int (integer type)
- Used by:
  - PartitionDesc (typedef pointer to PartitionDescData)
  - RelationBuildPartitionDesc (function that builds partition descriptors)

## Notes and Other Information
- For partitioned tables with detached partitions, PostgreSQL only caches descriptors that include all partitions. When a descriptor without detached partitions is requested, it's created fresh each time to handle snapshot-dependent visibility
- The caching mechanism is specifically optimized for get_partition_for_tuple() operations, which are frequently called during tuple routing in INSERT operations
- The last_found_count field behavior differs between LIST and RANGE partitioning strategies, optimizing for the different access patterns of each partitioning method
- This structure is defined in src/include/partitioning/partdesc.h:29-64 and is central to PostgreSQL's table partitioning infrastructure