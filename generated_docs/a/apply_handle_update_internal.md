# apply_handle_update_internal

## Location
[src/backend/replication/logical/worker.c:2643-2709](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L2643-L2709)

## Overview
Internal workhorse function that performs the actual UPDATE operation for logical replication, including tuple lookup, data modification, and execution within the proper concurrency control framework.

## Definition


## Detailed Description
This function performs the core UPDATE operation for logical replication after the higher-level apply_handle_update has handled message parsing and setup. The function:

1. **EPQ Setup**: Initializes EvalPlanQual state for proper concurrency control and snapshot management
2. **Index Management**: Opens all indexes on the target relation for update operations
3. **Tuple Lookup**: Uses FindReplTupleInLocalRel to locate the existing tuple in the local relation that matches the remote tuple
4. **Data Processing**: If the tuple is found, processes the new tuple data using slot_modify_data to apply column mappings and transformations
5. **Privilege Checking**: Verifies UPDATE privileges on the target relation
6. **Update Execution**: Performs the actual update using ExecSimpleRelationUpdate within the EPQ framework
7. **Error Handling**: Logs a debug message if the tuple to update cannot be found (rather than failing)
8. **Cleanup**: Properly closes indexes and EPQ state

The function is designed to handle both direct table updates and partition-specific updates when called from partition routing logic.

## Parameters / Member Variables
- : ApplyExecutionData structure containing execution state and relation mapping information
- : ResultRelInfo for the actual relation being updated (may be a partition of the target relation)
- : TupleTableSlot containing the search tuple data from the remote publisher
- : LogicalRepTupleData containing the new tuple values to be applied
- : OID of the local index to use for tuple lookup operations

## Dependencies
- Functions called/Symbols referenced:
  - [EvalPlanQualInit](../E/EvalPlanQualInit.md)
  - [ExecOpenIndices](../E/ExecOpenIndices.md)
  - [FindReplTupleInLocalRel](../F/FindReplTupleInLocalRel.md)
  - ExecClearTuple
  - GetPerTupleMemoryContext
  - [slot_modify_data](../s/slot_modify_data.md)
  - EvalPlanQualSetSlot
  - [TargetPrivilegesCheck](../T/TargetPrivilegesCheck.md)
  - [ExecSimpleRelationUpdate](../E/ExecSimpleRelationUpdate.md)
  - [ExecCloseIndices](../E/ExecCloseIndices.md)
  - [EvalPlanQualEnd](../E/EvalPlanQualEnd.md)
- Called from (representative examples):
  - [apply_handle_update](apply_handle_update.md)

## Notes and Other Information
- The function includes a comment noting that updates will fail if there are other conflicting unique indexes beyond the one used for tuple lookup
- When a tuple cannot be found for update, the function emits a DEBUG1 log message rather than throwing an error, allowing replication to continue
- The function properly manages memory contexts by switching to GetPerTupleMemoryContext for tuple data operations
- EPQ (EvalPlanQual) integration ensures proper handling of concurrent transactions and snapshot isolation
- The function is designed to work with both regular tables and partitioned tables through the relinfo parameter
- There's an XXX comment suggesting that the missing tuple case might be promoted to a higher log level in the future