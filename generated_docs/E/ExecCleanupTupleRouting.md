# ExecCleanupTupleRouting

## Location
[src/backend/executor/execPartition.c:1233-1293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L1233-L1293)

## Overview
Cleans up objects allocated for partition tuple routing by closing all partitioned tables, leaf partitions, and their indices when tuple routing operations are complete.

## Definition
```c
void ExecCleanupTupleRouting(ModifyTableState *mtstate, PartitionTupleRouting *proute)
```

## Detailed Description
This function performs cleanup operations for partition tuple routing by systematically closing resources that were opened during partitioned table operations. It handles two main categories of cleanup:

1. **Partition Dispatch Cleanup**: Closes intermediate partitioned tables (excluding the root table) and cleans up their associated tuple slots
2. **Leaf Partition Cleanup**: Closes leaf partitions, shuts down FDW operations if applicable, and closes indices

The function carefully avoids closing the root partitioned table (index 0 in partition_dispatch_info) since it is the main target table that will be closed by higher-level callers like ExecEndPlan() or DoCopy(). It also respects the borrowing relationship of result relations to avoid double-closing tables that belong to the owning ModifyTableState.

## Parameters / Member Variables
- `mtstate`: ModifyTableState containing the execution state for the modify operation
- `proute`: PartitionTupleRouting structure containing partition routing information including dispatch info and partition arrays

## Dependencies
- Functions called/Symbols referenced:
  - table_close
  - [ExecDropSingleTupleTableSlot](ExecDropSingleTupleTableSlot.md)
  - [ExecCloseIndices](ExecCloseIndices.md)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md) (during COPY operations)
  - [ExecEndModifyTable](ExecEndModifyTable.md) (during modify table cleanup)
  - [finish_edata](../f/finish_edata.md) (during logical replication worker cleanup)

## Notes and Other Information
- The root partitioned table (proute->partition_dispatch_info[0]) is intentionally skipped during cleanup as it will be closed by the calling context
- The function checks `is_borrowed_rel` flag to avoid closing relations that belong to the owning ModifyTableState
- FDW (Foreign Data Wrapper) shutdown is properly handled for foreign partitions through the EndForeignInsert callback
- All table closures use NoLock since the locks were acquired during the initial setup phase