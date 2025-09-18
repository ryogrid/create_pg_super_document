# change_cb_wrapper

## Location
src/backend/replication/logical/logical.c: 1105 - 1143

## Overview
A wrapper function that provides error handling and context management when calling logical replication output plugin change callbacks during logical decoding.

## Definition


## Detailed Description
The  function serves as an intermediary layer between PostgreSQL's logical replication infrastructure and output plugin change callbacks. It establishes proper error context, manages output state, and ensures that LSN (Log Sequence Number) information is correctly propagated for client communication. This wrapper is crucial for maintaining consistency and providing meaningful error messages during logical decoding operations.

The function sets up an error callback context that will provide detailed information if the plugin's change callback fails. It also manages the logical decoding context's output state, including setting the current transaction ID and LSN position for proper client synchronization.

## Parameters / Member Variables
- : ReorderBuffer instance containing the logical decoding state and private plugin data
- : ReorderBufferTXN representing the current transaction being processed
- : Relation object representing the table being modified
- : ReorderBufferChange containing the specific change details (INSERT, UPDATE, DELETE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](../R/ReorderBuffer.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
  - [ReorderBufferChange](../R/ReorderBufferChange.md)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - LogicalErrorCallbackState
  - output_plugin_error_callback
- Called from (representative examples):
  - StartupDecodingContext

## Notes and Other Information
- This function is only called when  is false, ensuring it's not used during fast-forward mode
- Sets  to enable output plugin writing
- Updates  with the change's LSN for client reply coordination
- Manages error context stack to provide meaningful error messages if the plugin callback fails
- The LSN tracking allows clients to acknowledge receipt of changes up to a specific point, enabling efficient replication confirmation
- Sets  to indicate this is not a transaction end event