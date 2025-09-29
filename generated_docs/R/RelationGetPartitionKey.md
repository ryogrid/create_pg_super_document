# RelationGetPartitionKey

## Location
[src/backend/utils/cache/partcache.c:51-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/partcache.c#L51-L77)

## Overview
Retrieves the partition key of a relation if it is a partitioned table, ensuring the partition key is built and cached for efficient access.

## Definition

```c
structure;
```
## Detailed Description
RelationGetPartitionKey is a fundamental function in PostgreSQL's partitioning system that provides access to a relation's partition key. The function performs lazy initialization of the partition key - if the partition key hasn't been built yet (rd_partkey is NULL), it calls RelationBuildPartitionKey to construct it. 

The function includes an important optimization: partition keys are immutable after a partitioned relation is created, so the relcache system preserves rd_partkey across relcache rebuilds as long as the relation remains open. This allows the function to safely return a direct pointer to the cached partition key rather than creating a copy.

## Parameters / Member Variables
- : The relation (table) for which to retrieve the partition key

## Dependencies
- Functions called/Symbols referenced:
  - [RelationBuildPartitionKey](RelationBuildPartitionKey.md) (called when rd_partkey is NULL)
- Called from (representative examples):
  - [has_partition_attrs](../h/has_partition_attrs.md) (src/backend/catalog/partition.c:266)
  - [DefineIndex](../D/DefineIndex.md) (src/backend/commands/indexcmds.c:949)  
  - [ExecInitPartitionDispatchInfo](../E/ExecInitPartitionDispatchInfo.md) (src/backend/executor/execPartition.c:1134)
  - [CreatePartitionPruneState](../C/CreatePartitionPruneState.md) (src/backend/executor/execPartition.c:1940)
  - [find_partition_scheme](../f/find_partition_scheme.md) (src/backend/optimizer/util/plancat.c:2451)
  - [transformPartitionCmd](../t/transformPartitionCmd.md) (src/backend/parser/parse_utilcmd.c:3940)
  - [get_qual_from_partbound](../g/get_qual_from_partbound.md) (src/backend/partitioning/partbounds.c:251)
  - [RelationBuildPartitionDesc](RelationBuildPartitionDesc.md) (src/backend/partitioning/partdesc.c:149)

## Notes and Other Information
- Returns NULL immediately if the relation is not a partitioned table (relkind != RELKIND_PARTITIONED_TABLE)
- Uses lazy initialization pattern - partition key is only built when first accessed
- The returned pointer remains valid as long as the relation is kept open due to relcache preservation
- Safe to use the returned pointer directly without copying since partition keys are immutable
- Uses unlikely() macro hint for the case where rd_partkey is NULL, indicating this should be rare after initial access

## Simplified Source

```c
PartitionKey RelationGetPartitionKey(Relation rel) {
    // Only partitioned tables have partition keys
    if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) {
        return NULL;
    }

    // Build partition key if not already cached
    if (unlikely(rel->rd_partkey == NULL)) {
        RelationBuildPartitionKey(rel);
    }

    return rel->rd_partkey;
}
```