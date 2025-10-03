# apply_handle_stream_abort

## Location
[src/backend/replication/logical/worker.c:1814-1970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1814-L1970)

## Overview
Handles the STREAM ABORT message in logical replication, coordinating the abort of streaming transactions between leader and parallel apply workers with different strategies based on the current transaction state.

## Definition

```c
static void
apply_handle_stream_abort(StringInfo s)
```
## Detailed Description
This function processes STREAM ABORT messages during logical replication, which signal the abort of either a complete streaming transaction or a subtransaction rollback. It performs different actions based on the current transaction apply strategy:

1. **TRANS_LEADER_APPLY**: Calls stream_abort_internal to handle file cleanup and truncation for serialized transactions
2. **TRANS_LEADER_SEND_TO_PARALLEL**: Attempts to send the abort message to a parallel worker with complex locking to handle subtransaction aborts, falls back to serialization mode if needed
3. **TRANS_LEADER_PARTIAL_SERIALIZE**: Writes the abort message to the spool file for later processing by parallel workers
4. **TRANS_PARALLEL_APPLY**: Closes stream files if needed and calls pa_stream_abort to handle the abort in the parallel worker

The function includes sophisticated handling of XID wraparound concerns and maintains proper synchronization between leader and parallel workers during abort processing.

## Parameters / Member Variables
- `s`: StringInfo containing the STREAM ABORT message data to be processed
## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_read_stream_abort](../l/logicalrep_read_stream_abort.md)
  - [set_apply_error_context_xact](../s/set_apply_error_context_xact.md)
  - [get_transaction_apply_action](../g/get_transaction_apply_action.md)
  - [stream_abort_internal](../s/stream_abort_internal.md)
  - [pa_unlock_stream](../p/pa_unlock_stream.md)
  - [pa_lock_stream](../p/pa_lock_stream.md)
  - [pa_send_data](../p/pa_send_data.md)
  - [pa_xact_finish](../p/pa_xact_finish.md)
  - [pa_switch_to_partial_serialize](../p/pa_switch_to_partial_serialize.md)
  - [stream_open_and_write_change](../s/stream_open_and_write_change.md)
  - [pa_set_fileset_state](../p/pa_set_fileset_state.md)
  - [stream_close_file](../s/stream_close_file.md)
  - [pa_stream_abort](../p/pa_stream_abort.md)
  - [pa_decr_and_wait_stream_block](../p/pa_decr_and_wait_stream_block.md)
  - [reset_apply_error_context_info](../r/reset_apply_error_context_info.md)
- Called from:
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- Validates that no streaming transaction is currently active (expects STREAM STOP first)
- Distinguishes between top-level transaction aborts and subtransaction rollbacks
- Implements careful locking protocol for subtransaction aborts in parallel mode
- Includes detailed comments about XID wraparound handling and duplicate entry prevention
- For top-level aborts, waits for parallel workers to finish to prevent XID conflicts
- Handles both serialized and parallel apply scenarios with appropriate cleanup
- Part of the logical replication streaming transaction abort protocol
- Critical for maintaining data consistency during transaction rollbacks and failures