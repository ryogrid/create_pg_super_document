# apply_handle_delete

## Location
src/backend/replication/logical/worker.c: 2710 - 2803

## Overview
Handles DELETE messages in PostgreSQL logical replication by applying the delete operation to the appropriate local table or partition, ensuring proper security context switching and tuple routing.

## Definition


## Detailed Description
This function is the main entry point for processing DELETE messages received from a logical replication publisher. It performs several key operations:

1. **Message Parsing**: Reads the DELETE message from the StringInfo buffer to extract the relation ID and old tuple data that identifies the tuple to be deleted
2. **Relation Validation**: Opens the target relation and checks if changes should be applied based on subscription settings
3. **Security Context**: Switches to the table owner's security context unless the subscription is configured to run as the subscriber owner
4. **Executor Initialization**: Sets up the execution environment and creates tuple slots for processing
5. **Search Tuple Building**: Constructs the search tuple using the old tuple data to locate the target tuple for deletion
6. **Tuple Routing**: For partitioned tables, routes the delete to the correct partition using apply_handle_tuple_routing; for regular tables, directly opens indices and calls apply_handle_delete_internal
7. **Cleanup**: Performs proper cleanup of resources and context restoration

The function includes early exit conditions for skipped changes or streamed transaction handling, and ensures proper error handling through callback mechanisms.

## Parameters / Member Variables
- : StringInfo buffer containing the serialized DELETE message from the publisher

## Dependencies
- Functions called/Symbols referenced:
  - is_skipping_changes
  - handle_streamed_transaction
  - begin_replication_step
  - logicalrep_read_delete
  - logicalrep_rel_open
  - should_apply_changes_for_rel
  - check_relation_updatable
  - SwitchToUntrustedUser
  - create_edata_for_relation
  - ExecInitExtraTupleSlot
  - slot_store_data
  - apply_handle_tuple_routing
  - ExecOpenIndices
  - apply_handle_delete_internal
  - ExecCloseIndices
  - finish_edata
  - RestoreUserContext
  - logicalrep_rel_close
  - end_replication_step
- Called from (representative examples):
  - apply_dispatch

## Notes and Other Information
- The function includes a TODO comment indicating that FDW (Foreign Data Wrapper) support is planned but not yet implemented
- For partitioned tables, the function delegates to apply_handle_tuple_routing to handle partition selection, passing NULL as the new tuple data since this is a DELETE operation
- For regular tables, it directly manages index operations and calls apply_handle_delete_internal to perform the actual deletion
- The function carefully manages memory contexts and security contexts to ensure proper isolation
- Error handling is managed through the apply_error_callback_arg global structure
- Unlike UPDATE operations, DELETE operations only require the old tuple data to identify the target tuple for removal