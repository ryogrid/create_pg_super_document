# apply_handle_stream_stop

## Location
src/backend/replication/logical/worker.c: 1628 - 1730

## Overview
Handles the STREAM STOP message in logical replication, finalizing a streaming transaction and coordinating between leader and parallel apply workers to complete transaction processing.

## Definition


## Detailed Description
This function processes STREAM STOP messages during logical replication, which signal the end of a streaming transaction. It performs different actions based on the current transaction apply strategy:

1. **TRANS_LEADER_SERIALIZE**: Directly calls stream_stop_internal to finalize the serialized transaction
2. **TRANS_LEADER_SEND_TO_PARALLEL**: Attempts to send the STOP message to a parallel worker, with fallback to serialization mode if sending fails
3. **TRANS_LEADER_PARTIAL_SERIALIZE**: Writes the STOP message to the spool file and finalizes the transaction
4. **TRANS_PARALLEL_APPLY**: Decrements the stream block count and waits for more work if needed

The function includes sophisticated locking mechanisms to coordinate between leader and parallel apply workers, ensuring proper synchronization during transaction completion. After processing, it cleans up the streaming transaction state and updates the worker's activity status.

## Parameters / Member Variables
- : StringInfo containing the STREAM STOP message data to be processed

## Dependencies
- Functions called/Symbols referenced:
  - get_transaction_apply_action
  - stream_stop_internal
  - pa_lock_stream
  - pa_send_data
  - pa_set_stream_apply_worker
  - pa_switch_to_partial_serialize
  - stream_write_change
  - pa_decr_and_wait_stream_block
  - IsTransactionOrTransactionBlock
  - pgstat_report_activity
  - reset_apply_error_context_info
- Called from:
  - apply_dispatch

## Notes and Other Information
- Validates that a STREAM START message was previously received
- Resets global streaming transaction state (in_streamed_transaction and stream_xid)
- Includes complex locking logic to prevent race conditions between leader and parallel workers
- Reports appropriate activity state (IDLE or IDLEINTRANSACTION) based on current transaction status
- Contains detailed comments about race conditions and their handling in parallel apply scenarios
- Part of the logical replication streaming transaction completion protocol