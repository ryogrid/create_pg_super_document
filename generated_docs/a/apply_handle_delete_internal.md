# apply_handle_delete_internal

## Location
[src/backend/replication/logical/worker.c:2804-2860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L2804-L2860)

## Overview
Internal workhorse function that performs the actual DELETE operation for logical replication, including tuple lookup and execution within the proper concurrency control framework.

## Definition


## Detailed Description
This function performs the core DELETE operation for logical replication after the higher-level apply_handle_delete has handled message parsing and setup. The function:

1. **EPQ Setup**: Initializes EvalPlanQual state for proper concurrency control and snapshot management
2. **Index Validation**: Asserts that the caller has already opened the necessary indexes on the target relation
3. **Tuple Lookup**: Uses FindReplTupleInLocalRel to locate the existing tuple in the local relation that matches the remote tuple to be deleted
4. **EPQ Integration**: If the tuple is found, sets up the EPQ state with the local tuple slot
5. **Privilege Checking**: Verifies DELETE privileges on the target relation
6. **Delete Execution**: Performs the actual deletion using ExecSimpleRelationDelete within the EPQ framework
7. **Error Handling**: Logs a debug message if the tuple to delete cannot be found (rather than failing)
8. **Cleanup**: Properly ends the EPQ state

The function is designed to handle both direct table deletions and partition-specific deletions when called from partition routing logic.

## Parameters / Member Variables
- : ApplyExecutionData structure containing execution state and relation mapping information
- : ResultRelInfo for the actual relation being deleted from (may be a partition of the target relation)
- : TupleTableSlot containing the search tuple data from the remote publisher to identify the tuple to delete
- : OID of the local index to use for tuple lookup operations

## Dependencies
- Functions called/Symbols referenced:
  - [EvalPlanQualInit](../E/EvalPlanQualInit.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md) (used in assertion)
  - [FindReplTupleInLocalRel](../F/FindReplTupleInLocalRel.md)
  - EvalPlanQualSetSlot
  - [TargetPrivilegesCheck](../T/TargetPrivilegesCheck.md)
  - [ExecSimpleRelationDelete](../E/ExecSimpleRelationDelete.md)
  - [EvalPlanQualEnd](../E/EvalPlanQualEnd.md)
- Called from (representative examples):
  - [apply_handle_delete](apply_handle_delete.md)
  - [apply_handle_tuple_routing](apply_handle_tuple_routing.md)

## Notes and Other Information
- The function includes an assertion to verify that the caller has properly opened indexes before calling this function, as index management is handled at a higher level
- When a tuple cannot be found for deletion, the function emits a DEBUG1 log message rather than throwing an error, allowing replication to continue gracefully
- There's an XXX comment suggesting that the missing tuple case might be promoted to a higher log level in the future
- EPQ (EvalPlanQual) integration ensures proper handling of concurrent transactions and snapshot isolation
- The function is more lightweight than its UPDATE counterpart since it doesn't need to handle data modification or column mapping
- Unlike the update internal function, this one doesn't need to manage memory contexts for tuple data modification since it only performs lookups and deletions
- The function is called from both direct delete operations and partition routing scenarios