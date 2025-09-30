# ExecLookupResultRelByOid

## Location
[src/backend/executor/nodeModifyTable.c:4373-4421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L4373-L4421)

## Overview
Locates a ResultRelInfo structure for a specific table OID among the result relations managed by a ModifyTable node, providing efficient lookup for DML operations.

## Definition

```c
ResultRelInfo *
ExecLookupResultRelByOid(ModifyTableState *node, Oid resultoid,
						 bool missing_ok, bool update_cache)
```
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
  - [exec_rt_fetch](../e/exec_rt_fetch.md)

## Notes and Other Information
- The function uses a hybrid approach for performance: hash table lookup for many relations, linear search for few relations
- Only ExecModifyTable should pass  to maintain cache consistency
- The caching mechanism stores the last looked-up OID and its corresponding index for quick subsequent access
- Error handling respects the  parameter, allowing callers to handle missing relations gracefully or fail fast as needed
- This function is critical for partition-wise operations where different tuples may target different result relations

## Simplified Source

```c
ResultRelInfo *
ExecLookupResultRelByOid(ModifyTableState *node, Oid resultoid,
                         bool missing_ok, bool update_cache)
{
    if (node->mt_resultOidHash) {
        // Use hash table for efficient lookup with many relations
        MTTargetRelLookup *mtlookup;

        mtlookup = (MTTargetRelLookup *)
            hash_search(node->mt_resultOidHash, &resultoid, HASH_FIND, NULL);
        if (mtlookup) {
            if (update_cache) {
                node->mt_lastResultOid = resultoid;
                node->mt_lastResultIndex = mtlookup->relationIndex;
            }
            return node->resultRelInfo + mtlookup->relationIndex;
        }
    } else {
        // Linear search for few relations
        for (int ndx = 0; ndx < node->mt_nrels; ndx++) {
            ResultRelInfo *rInfo = node->resultRelInfo + ndx;

            if (RelationGetRelid(rInfo->ri_RelationDesc) == resultoid) {
                if (update_cache) {
                    node->mt_lastResultOid = resultoid;
                    node->mt_lastResultIndex = ndx;
                }
                return rInfo;
            }
        }
    }

    // Handle not found case
    if (!missing_ok)
        elog(ERROR, "incorrect result relation OID %u", resultoid);
    return NULL;
}
```