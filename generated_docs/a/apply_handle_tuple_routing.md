# apply_handle_tuple_routing

## Location
[src/backend/replication/logical/worker.c:2908-3156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L2908-L3156)

## Overview
Handles insert, update, and delete operations on partitioned tables in PostgreSQL's logical replication worker, managing tuple routing to appropriate partitions.

## Definition

```c
static void
apply_handle_tuple_routing(ApplyExecutionData *edata,
						   TupleTableSlot *remoteslot,
						   LogicalRepTupleData *newtup,
						   CmdType operation)
```
## Detailed Description
This function implements the core logic for applying DML operations (INSERT, UPDATE, DELETE) on partitioned tables in logical replication. It handles the complex process of:

1. **Partition Discovery**: Uses tuple routing to find the correct partition for the incoming tuple
2. **Tuple Conversion**: Handles rowtype conversions between parent and child partitions when schemas differ
3. **Operation-Specific Logic**: 
   - For INSERT: Directly inserts into the target partition
   - For DELETE: Deletes from the appropriate partition
   - For UPDATE: Implements sophisticated logic that may result in either an in-place update or a cross-partition move (DELETE from old + INSERT into new)

The function sets up the necessary execution state including ModifyTableState and PartitionTupleRouting structures, then delegates to partition-specific internal functions for the actual DML operations.

For UPDATE operations, it performs additional validation to check if the updated tuple still satisfies the current partition's constraints. If not, it performs a cross-partition move by deleting the old tuple and inserting the new tuple into the correct partition.

## Parameters / Member Variables
- : ApplyExecutionData structure containing execution context and target relation information
- : TupleTableSlot containing the incoming tuple from the remote publisher
- : LogicalRepTupleData containing new tuple data (used for UPDATE operations)
- : CmdType indicating the DML operation (CMD_INSERT, CMD_UPDATE, or CMD_DELETE)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecSetupPartitionTupleRouting](../E/ExecSetupPartitionTupleRouting.md)
  - [ExecFindPartition](../E/ExecFindPartition.md)
  - [CheckSubscriptionRelkind](../C/CheckSubscriptionRelkind.md)
  - [ExecGetRootToChildMap](../E/ExecGetRootToChildMap.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [apply_handle_insert_internal](apply_handle_insert_internal.md)
  - [apply_handle_delete_internal](apply_handle_delete_internal.md)
  - [FindReplTupleInLocalRel](../F/FindReplTupleInLocalRel.md)
  - [slot_modify_data](../s/slot_modify_data.md)
  - [ExecPartitionCheck](../E/ExecPartitionCheck.md)
  - [convert_tuples_by_name](../c/convert_tuples_by_name.md)
- Called from (representative examples):
  - [apply_handle_insert](apply_handle_insert.md)
  - [apply_handle_update](apply_handle_update.md)
  - [apply_handle_delete](apply_handle_delete.md)

## Notes and Other Information
- This function is crucial for logical replication of partitioned tables, ensuring data consistency across partition boundaries
- Handles complex scenarios like cross-partition UPDATEs where a tuple needs to move between partitions
- Includes comprehensive error handling and validation of partition constraints
- Uses memory context switching to manage temporary allocations during tuple processing
- The function validates that partitions have supported relation kinds for replication
- For cross-partition moves, it performs both DELETE and INSERT operations atomically within the same transaction