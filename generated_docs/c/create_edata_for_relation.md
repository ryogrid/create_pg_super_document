# create_edata_for_relation

## Location
[src/backend/replication/logical/worker.c:654-710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L654-L710)

## Overview
Creates and initializes executor state data (ApplyExecutionData) for evaluation of constraint expressions, indexes, and triggers for a specific relation during logical replication operations.

## Definition


## Detailed Description
This function sets up the executor state infrastructure required for applying logical replication changes to a specific relation. It creates an ApplyExecutionData structure that encapsulates all the necessary executor state components including:

- An EState (executor state) for managing the execution context
- A RangeTblEntry for the target relation with appropriate permissions
- A ResultRelInfo structure initialized for the target relation
- Proper setup of result relations list for trigger execution
- Command ID assignment for transaction visibility
- Initialization of AFTER trigger processing

The function is specifically designed for logical replication workers and ensures that all necessary executor infrastructure is properly initialized before applying INSERT, UPDATE, or DELETE operations to the target relation.

## Parameters / Member Variables
- : A LogicalRepRelMapEntry pointer containing the mapping information for the target relation, including both the local relation handle and replication metadata

## Dependencies
- Functions called/Symbols referenced:
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - makeNode
  - RelationGetRelid
  - [addRTEPermissionInfo](../a/addRTEPermissionInfo.md)
  - ExecInitRangeTable
  - [InitResultRelInfo](../I/InitResultRelInfo.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - [AfterTriggerBeginQuery](../A/AfterTriggerBeginQuery.md)
- Called from (representative examples):
  - [apply_handle_insert](../a/apply_handle_insert.md)
  - [apply_handle_update](../a/apply_handle_update.md)
  - [apply_handle_delete](../a/apply_handle_delete.md)

## Notes and Other Information
- This is a static function used internally within the logical replication worker
- The caller is responsible for opening and closing any indexes that need to be updated
- The function sets up the ResultRelInfo in es_opened_result_relations list to make it discoverable by ExecGetTriggerResultRel()
- [ExecOpenIndices](../E/ExecOpenIndices.md)() is deliberately not called here - each execution path is responsible for index management
- The function prepares the system to catch AFTER triggers by calling AfterTriggerBeginQuery()
- Uses AccessShareLock as the relation lock mode in the range table entry
- The returned ApplyExecutionData structure has most fields initialized to NULL initially, with specific fields populated as needed by subsequent operations