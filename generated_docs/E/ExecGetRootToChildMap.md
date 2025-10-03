# ExecGetRootToChildMap

## Location
[src/backend/executor/execUtils.c:1232-1266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L1232-L1266)

## Overview
Returns the tuple conversion map needed to convert tuples from a root result relation's rowtype to the rowtype of a child relation, handling schema differences between parent and child tables in partitioned table hierarchies.

## Definition

```c
TupleConversionMap *
ExecGetRootToChildMap(ResultRelInfo *resultRelInfo, EState *estate)
```
## Detailed Description
This function computes and caches a TupleConversionMap that enables conversion of tuples from the root table's schema to a child table's schema in partitioned table operations. The function implements lazy evaluation - it only computes the conversion map when first requested and caches the result for subsequent calls.

The function handles two main scenarios:
1. **Partitioned tables**: Child partitions must have compatible column layouts with the parent
2. **Non-partitioned child tables**: May have additional columns not present in the root table, which are handled gracefully

The conversion map accounts for differences in column order, data types, and presence/absence of columns between the root and child relations. When no conversion is needed (schemas are identical), the function returns NULL as an optimization.

## Parameters / Member Variables
- `*resultRelInfo`: ResultRelInfo structure for the child relation that needs tuple conversion
- `*estate`: Executor state containing memory context and other execution information
## Dependencies
- Functions called/Symbols referenced:
  - [build_attrmap_by_name_if_req](../b/build_attrmap_by_name_if_req.md): Creates attribute mapping between input and output tuple descriptors
  - [convert_tuples_by_name_attrmap](../c/convert_tuples_by_name_attrmap.md): Converts the attribute map into a tuple conversion map
  - [AttrMap](../A/AttrMap.md): Attribute mapping structure for column correspondence
- Called from (representative examples):
  - [ExecGetInsertedCols](ExecGetInsertedCols.md): For converting inserted column bitmaps during partition operations
  - [ExecGetUpdatedCols](ExecGetUpdatedCols.md): For converting updated column bitmaps during partition operations
  - [ExecFindPartition](ExecFindPartition.md): During tuple routing to determine target partition
  - [CopyFrom](../C/CopyFrom.md): When copying data into partitioned tables

## Notes and Other Information
- The function uses lazy initialization with the  flag to avoid recomputing the map
- Memory allocation occurs in the query context () to ensure proper lifetime management
- For non-partitioned child relations, missing columns in the child are handled by setting 
- The function assumes the caller has verified that  represents a child relation (assertion check)
- A NULL return value is valid and indicates no conversion is necessary between root and child schemas

## Simplified Source

```c
TupleConversionMap *
ExecGetRootToChildMap(ResultRelInfo *resultRelInfo, EState *estate)
{
    // Must be called for a child relation
    Assert(resultRelInfo->ri_RootResultRelInfo);

    // Compute map if not already done
    if (!resultRelInfo->ri_RootToChildMapValid)
    {
        ResultRelInfo *rootRelInfo = resultRelInfo->ri_RootResultRelInfo;
        TupleDesc indesc = RelationGetDescr(rootRelInfo->ri_RelationDesc);
        TupleDesc outdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
        Relation childrel = resultRelInfo->ri_RelationDesc;
        MemoryContext oldcontext;

        // Switch to query context for permanent allocation
        oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

        // Build attribute map between root and child schemas
        // For non-partitions, allow missing columns in child
        AttrMap *attrMap = build_attrmap_by_name_if_req(indesc, outdesc,
                                                       !childrel->rd_rel->relispartition);

        // Create tuple conversion map if needed
        if (attrMap)
            resultRelInfo->ri_RootToChildMap = convert_tuples_by_name_attrmap(indesc, outdesc, attrMap);

        MemoryContextSwitchTo(oldcontext);
        resultRelInfo->ri_RootToChildMapValid = true;
    }

    return resultRelInfo->ri_RootToChildMap;
}
```