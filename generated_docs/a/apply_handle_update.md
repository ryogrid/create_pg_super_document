# apply_handle_update

## Location
src/backend/replication/logical/worker.c: 2526 - 2642

## Overview
Handles UPDATE messages in PostgreSQL logical replication by applying the update operation to the appropriate local table or partition, ensuring proper security context switching and tuple routing.

## Definition


## Detailed Description
This function is the main entry point for processing UPDATE messages received from a logical replication publisher. It performs several key operations:

1. **Message Parsing**: Reads the UPDATE message from the StringInfo buffer to extract relation ID, old tuple data (if present), and new tuple data
2. **Relation Validation**: Opens the target relation and checks if changes should be applied based on subscription settings
3. **Security Context**: Switches to the table owner's security context unless the subscription is configured to run as the subscriber owner
4. **Executor Initialization**: Sets up the execution environment and creates tuple slots for processing
5. **Column Tracking**: Populates the updatedCols bitmap to track which columns are being modified for proper trigger firing
6. **Tuple Routing**: For partitioned tables, routes the update to the correct partition; otherwise directly applies the update
7. **Cleanup**: Performs proper cleanup of resources and context restoration

The function includes early exit conditions for skipped changes or streamed transaction handling, and ensures proper error handling through callback mechanisms.

## Parameters / Member Variables
- : StringInfo buffer containing the serialized UPDATE message from the publisher

## Dependencies
- Functions called/Symbols referenced:
  - is_skipping_changes
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)
  - [begin_replication_step](../b/begin_replication_step.md)
  - logicalrep_read_update
  - logicalrep_rel_open
  - [should_apply_changes_for_rel](../s/should_apply_changes_for_rel.md)
  - [check_relation_updatable](../c/check_relation_updatable.md)
  - [SwitchToUntrustedUser](../S/SwitchToUntrustedUser.md)
  - [create_edata_for_relation](../c/create_edata_for_relation.md)
  - [ExecInitExtraTupleSlot](../E/ExecInitExtraTupleSlot.md)
  - [slot_store_data](../s/slot_store_data.md)
  - [apply_handle_tuple_routing](apply_handle_tuple_routing.md)
  - [apply_handle_update_internal](apply_handle_update_internal.md)
  - [finish_edata](../f/finish_edata.md)
  - [RestoreUserContext](../R/RestoreUserContext.md)
  - logicalrep_rel_close
  - [end_replication_step](../e/end_replication_step.md)
- Called from (representative examples):
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- The function includes a TODO comment indicating that FDW (Foreign Data Wrapper) support is planned but not yet implemented
- For partitioned tables, the function delegates to apply_handle_tuple_routing to handle partition selection
- For regular tables, it directly calls apply_handle_update_internal to perform the actual update
- The function carefully manages memory contexts and security contexts to ensure proper isolation
- Column change tracking supports per-column triggers and executor optimizations like indexUnchanged hints
- Error handling is managed through the apply_error_callback_arg global structure