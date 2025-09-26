# PartitionDispatch

## Location
[src/include/executor/execPartition.h:22-22](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/execPartition.h#L22-L22)

## Overview
PartitionDispatch is a type definition for a pointer to PartitionDispatchData structure, which contains information about one partitioned table in a partition hierarchy required to route tuples to any of its partitions.

## Definition

```c
typedef struct PartitionDispatchData *PartitionDispatch;
```
## Detailed Description
PartitionDispatch serves as a handle to access partition dispatch information for a specific partitioned table within PostgreSQL's partition routing system. It encapsulates all the metadata and execution state needed to determine which partition a tuple should be routed to, including the partition key, partition descriptor, and routing indexes. This structure is always stored within a PartitionTupleRouting's partition_dispatch_info array and is essential for the tuple routing mechanism in partitioned tables.

## Parameters / Member Variables
The underlying PartitionDispatchData structure contains:
- : Relation descriptor of the partitioned table
- : Partition key information defining how tuples are partitioned
- : Execution state required for expressions in the partition key (list of ExprState)
- : Partition descriptor containing metadata about child partitions
- : TupleTableSlot initialized with this table's tuple descriptor, or NULL if no tuple conversion is needed
- : TupleConversionMap for converting from parent's rowtype to this table's rowtype, or NULL if no conversion required
- : Array mapping partdesc entries to either ResultRelInfo indexes (for leaf partitions) or PartitionDispatch indexes (for partitioned partitions); -1 indicates unallocated

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionDispatchData](PartitionDispatchData.md) (underlying structure)
- Called from (representative examples):
  - [ExecFindPartition](../E/ExecFindPartition.md)
  - [ExecInitPartitionDispatchInfo](../E/ExecInitPartitionDispatchInfo.md)
  - [ExecInitRoutingInfo](../E/ExecInitRoutingInfo.md)
  - [FormPartitionKeyDatum](../F/FormPartitionKeyDatum.md)
  - [get_partition_for_tuple](../g/get_partition_for_tuple.md)

## Notes and Other Information
- [PartitionDispatch](PartitionDispatch.md) is always used as part of a larger PartitionTupleRouting structure
- The indexes array uses a flexible array member, allowing variable-length allocation based on the number of partitions
- This abstraction allows PostgreSQL to efficiently navigate complex partition hierarchies during INSERT, UPDATE, and COPY operations
- The structure supports both leaf partitions and nested partitioned partitions through its indexing scheme