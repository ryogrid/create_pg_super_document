# RelationGetPartitionKey

## Location
src/backend/utils/cache/partcache.c: 51 - 77

## Overview
Retrieves the partition key of a relation if it is a partitioned table, ensuring the partition key is built and cached for efficient access.

## Definition


## Detailed Description
RelationGetPartitionKey is a fundamental function in PostgreSQL's partitioning system that provides access to a relation's partition key. The function performs lazy initialization of the partition key - if the partition key hasn't been built yet (rd_partkey is NULL), it calls RelationBuildPartitionKey to construct it. 

The function includes an important optimization: partition keys are immutable after a partitioned relation is created, so the relcache system preserves rd_partkey across relcache rebuilds as long as the relation remains open. This allows the function to safely return a direct pointer to the cached partition key rather than creating a copy.

## Parameters / Member Variables
- : The relation (table) for which to retrieve the partition key

## Dependencies
- Functions called/Symbols referenced:
  - RelationBuildPartitionKey (called when rd_partkey is NULL)
- Called from (representative examples):
  - has_partition_attrs (src/backend/catalog/partition.c:266)
  - DefineIndex (src/backend/commands/indexcmds.c:949)  
  - ExecInitPartitionDispatchInfo (src/backend/executor/execPartition.c:1134)
  - CreatePartitionPruneState (src/backend/executor/execPartition.c:1940)
  - find_partition_scheme (src/backend/optimizer/util/plancat.c:2451)
  - transformPartitionCmd (src/backend/parser/parse_utilcmd.c:3940)
  - get_qual_from_partbound (src/backend/partitioning/partbounds.c:251)
  - RelationBuildPartitionDesc (src/backend/partitioning/partdesc.c:149)

## Notes and Other Information
- Returns NULL immediately if the relation is not a partitioned table (relkind != RELKIND_PARTITIONED_TABLE)
- Uses lazy initialization pattern - partition key is only built when first accessed
- The returned pointer remains valid as long as the relation is kept open due to relcache preservation
- Safe to use the returned pointer directly without copying since partition keys are immutable
- Uses unlikely() macro hint for the case where rd_partkey is NULL, indicating this should be rare after initial access