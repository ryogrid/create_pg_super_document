# PartitionDispatchData

## Location
[src/backend/executor/execPartition.c:143-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L143-L152)

## Overview
PartitionDispatchData contains information about one partitioned table in a partition hierarchy required to route a tuple to any of its partitions, encapsulated within a PartitionTupleRouting structure.

## Definition

```c
typedef struct PartitionDispatchData
{
	Relation	reldesc;
	PartitionKey key;
	List	   *keystate;		/* list of ExprState */
	PartitionDesc partdesc;
	TupleTableSlot *tupslot;
	AttrMap    *tupmap;
	int			indexes[FLEXIBLE_ARRAY_MEMBER];
}			PartitionDispatchData;
```
## Detailed Description
PartitionDispatchData represents the routing information needed for a single partitioned table within PostgreSQL's partition hierarchy. Each instance contains the table's relation descriptor, partition key information, execution state for partition key expressions, and mapping structures for tuple conversion. The structure includes a flexible array member 'indexes' that provides efficient lookup into the parent PartitionTupleRouting's arrays, coordinating access to both leaf partitions and nested partitioned tables.

## Parameters / Member Variables
- : Relation descriptor of the partitioned table
- : PartitionKey information defining how the table is partitioned
- : List of ExprState objects containing execution state for expressions in the partition key
- : PartitionDesc containing the partition descriptor information for the table
- : Standalone TupleTableSlot initialized with this table's tuple descriptor, or NULL if no tuple conversion is required
- : TupleConversionMap to convert from parent's rowtype to this table's rowtype during partition key extraction, NULL if no conversion needed
- : Flexible array with partdesc->nparts elements mapping partition indexes to ResultRelInfo (for leaf partitions) or PartitionDispatch (for sub-partitioned tables) in the encapsulating PartitionTupleRouting arrays, -1 indicates unallocated partitions

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionKey](PartitionKey.md)
  - [PartitionDesc](PartitionDesc.md)  
  - [AttrMap](../A/AttrMap.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [ExecInitPartitionDispatchInfo](../E/ExecInitPartitionDispatchInfo.md)
  - PartitionDispatch (typedef usage)

## Notes and Other Information
The structure uses a flexible array member for the indexes array, allowing it to be sized according to the number of partitions at allocation time. The indexes array serves as an indirection mechanism, mapping local partition numbers to global indexes in the parent PartitionTupleRouting structure's arrays. This design enables efficient partition lookup while maintaining a clean separation between dispatch information for individual tables and the overall routing context.