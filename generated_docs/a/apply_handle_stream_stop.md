# apply_handle_stream_stop

## Location
[src/backend/replication/logical/worker.c:1628-1730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1628-L1730)

## Overview
Handles the STREAM STOP message in logical replication, finalizing a streaming transaction and coordinating between leader and parallel apply workers to complete transaction processing.

## Definition

```c
static void
apply_handle_stream_stop(StringInfo s)
```
## Detailed Description
This function processes STREAM STOP messages during logical replication, which signal the end of a streaming transaction. It performs different actions based on the current transaction apply strategy:

1. **TRANS_LEADER_SERIALIZE**: Directly calls stream_stop_internal to finalize the serialized transaction
2. **TRANS_LEADER_SEND_TO_PARALLEL**: Attempts to send the STOP message to a parallel worker, with fallback to serialization mode if sending fails
3. **TRANS_LEADER_PARTIAL_SERIALIZE**: Writes the STOP message to the spool file and finalizes the transaction
4. **TRANS_PARALLEL_APPLY**: Decrements the stream block count and waits for more work if needed

The function includes sophisticated locking mechanisms to coordinate between leader and parallel apply workers, ensuring proper synchronization during transaction completion. After processing, it cleans up the streaming transaction state and updates the worker's activity status.

## Parameters / Member Variables
- `s`: StringInfo containing the STREAM STOP message data to be processed
## Dependencies
- Functions called/Symbols referenced:
  - [get_transaction_apply_action](../g/get_transaction_apply_action.md)
  - [stream_stop_internal](../s/stream_stop_internal.md)
  - [pa_lock_stream](../p/pa_lock_stream.md)
  - [pa_send_data](../p/pa_send_data.md)
  - [pa_set_stream_apply_worker](../p/pa_set_stream_apply_worker.md)
  - [pa_switch_to_partial_serialize](../p/pa_switch_to_partial_serialize.md)
  - [stream_write_change](../s/stream_write_change.md)
  - [pa_decr_and_wait_stream_block](../p/pa_decr_and_wait_stream_block.md)
  - [IsTransactionOrTransactionBlock](../I/IsTransactionOrTransactionBlock.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [reset_apply_error_context_info](../r/reset_apply_error_context_info.md)
- Called from:
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- Validates that a STREAM START message was previously received
- Resets global streaming transaction state (in_streamed_transaction and stream_xid)
- Includes complex locking logic to prevent race conditions between leader and parallel workers
- Reports appropriate activity state (IDLE or IDLEINTRANSACTION) based on current transaction status
- Contains detailed comments about race conditions and their handling in parallel apply scenarios
- Part of the logical replication streaming transaction completion protocol