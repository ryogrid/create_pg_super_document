# ExecPrepareTupleRouting

## Location
src/backend/executor/nodeModifyTable.c: 3893 - 3952

## Overview
Prepares for routing a tuple to the appropriate partition by determining the target partition and converting the tuple format if necessary.

## Definition
```c
static TupleTableSlot *ExecPrepareTupleRouting(ModifyTableState *mtstate,
                                               EState *estate,
                                               PartitionTupleRouting *proute,
                                               ResultRelInfo *targetRelInfo,
                                               TupleTableSlot *slot,
                                               ResultRelInfo **partRelInfo)
```

## Detailed Description
The ExecPrepareTupleRouting function handles the complex process of routing tuples to appropriate partitions in a partitioned table. It first determines the target partition using ExecFindPartition, then handles tuple format conversion between the root table's rowtype and the partition's rowtype when necessary. The function also manages transition table capture optimization by storing the original tuple when no BEFORE triggers are present on the partition that could modify it, avoiding unnecessary tuple conversions. If tuple conversion is required, it uses the attribute mapping to transform the tuple into the partition's expected format.

## Parameters / Member Variables
- `mtstate`: Pointer to ModifyTableState containing execution state and transition capture information
- `estate`: Pointer to EState containing the execution context
- `proute`: Pointer to PartitionTupleRouting containing partition routing information
- `targetRelInfo`: Pointer to ResultRelInfo for the target relation (root partitioned table)
- `slot`: TupleTableSlot containing the tuple to be routed
- `partRelInfo`: Output parameter that receives the pointer to the target partition's ResultRelInfo

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFindPartition](ExecFindPartition.md)
  - [ExecGetRootToChildMap](ExecGetRootToChildMap.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - TupleConversionMap (structure)
  - [PartitionTupleRouting](../P/PartitionTupleRouting.md) (structure)
- Called from (representative examples):
  - [ExecInsert](ExecInsert.md) (at src/backend/executor/nodeModifyTable.c:806)

## Notes and Other Information
- Returns a TupleTableSlot containing the tuple formatted for the target partition's rowtype
- The function optimizes transition table capture by avoiding tuple conversion when no BEFORE INSERT triggers exist on the partition
- Tuple conversion is performed using attribute maps that handle differences between root table and partition schemas
- The function can raise errors if no valid partition is found or if the found partition is not a valid target for the operation
- Essential for partitioned table operations where tuples may need to be routed to different partitions based on partition key values
- Located in src/backend/executor/nodeModifyTable.c:3893-3952