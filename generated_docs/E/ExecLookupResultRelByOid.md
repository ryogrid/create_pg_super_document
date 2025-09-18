# ExecLookupResultRelByOid

## Location
src/backend/executor/nodeModifyTable.c: 4373 - 4421

## Overview
Locates a ResultRelInfo structure for a specific table OID among the result relations managed by a ModifyTable node, providing efficient lookup for DML operations.

## Definition


## Detailed Description
This function searches for a ResultRelInfo structure corresponding to a given table OID within the result relations of a ModifyTable node. It employs two different search strategies based on the number of target relations: a hash table for efficient lookup when many relations are involved, or a simple linear search for fewer relations. The function also provides caching capabilities to optimize repeated lookups of the same relation.

The function first checks if a hash table () exists for fast lookups. If present, it uses  to find the target relation. If no hash table exists (typically for nodes with few target relations), it performs a linear search through the  array, comparing each relation's OID with the target OID.

## Parameters / Member Variables
- : ModifyTableState containing the result relations to search through
- : The OID of the target relation to locate
- : If true, return NULL when relation is not found; if false, raise an error
- : If true and lookup succeeds, update the node's one-element cache (should only be true when called from ExecModifyTable)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)
  - RelationGetRelid
  - elog
- Data structures used:
  - [ModifyTableState](../M/ModifyTableState.md)
  - [MTTargetRelLookup](../M/MTTargetRelLookup.md)
  - [ResultRelInfo](../R/ResultRelInfo.md)
- Called from (representative examples):
  - [ExecFindPartition](ExecFindPartition.md)
  - [ExecModifyTable](ExecModifyTable.md)
  - exec_rt_fetch

## Notes and Other Information
- The function uses a hybrid approach for performance: hash table lookup for many relations, linear search for few relations
- Only ExecModifyTable should pass  to maintain cache consistency
- The caching mechanism stores the last looked-up OID and its corresponding index for quick subsequent access
- Error handling respects the  parameter, allowing callers to handle missing relations gracefully or fail fast as needed
- This function is critical for partition-wise operations where different tuples may target different result relations