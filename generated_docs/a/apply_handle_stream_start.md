# apply_handle_stream_start

## Location
[src/backend/replication/logical/worker.c:1469-1604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1469-L1604)

## Overview
Handles the STREAM START message in logical replication, initiating the processing of a streaming transaction and determining the appropriate handling strategy (serialize, parallel apply, or send to parallel worker).

## Definition

```c
static void
apply_handle_stream_start(StringInfo s)
```
## Detailed Description
This function processes STREAM START messages during logical replication, which signal the beginning of a streaming transaction. It performs several critical tasks:

1. **Validation**: Ensures no duplicate STREAM START messages and validates the transaction ID
2. **Transaction Setup**: Extracts the transaction XID and sets up the streaming transaction context
3. **Worker Allocation**: For first segments, attempts to allocate a parallel worker if available
4. **Action Determination**: Decides how to handle the transaction based on system state and worker availability:
   - **TRANS_LEADER_SERIALIZE**: Serializes changes to a spool file for later processing
   - **TRANS_LEADER_SEND_TO_PARALLEL**: Sends data directly to a parallel apply worker
   - **TRANS_LEADER_PARTIAL_SERIALIZE**: Falls back to serialization when parallel sending fails
   - **TRANS_PARALLEL_APPLY**: Handles the transaction in a parallel apply worker

The function coordinates between the leader apply worker and parallel apply workers to optimize transaction processing performance.

## Parameters / Member Variables
- `s`: StringInfo containing the STREAM START message data to be processed
## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_read_stream_start](../l/logicalrep_read_stream_start.md)
  - [set_apply_error_context_xact](../s/set_apply_error_context_xact.md)
  - [pa_allocate_worker](../p/pa_allocate_worker.md)
  - [get_transaction_apply_action](../g/get_transaction_apply_action.md)
  - [stream_start_internal](../s/stream_start_internal.md)
  - [pa_send_data](../p/pa_send_data.md)
  - [pa_unlock_stream](../p/pa_unlock_stream.md)
  - [pa_switch_to_partial_serialize](../p/pa_switch_to_partial_serialize.md)
  - [stream_write_change](../s/stream_write_change.md)
  - [pa_set_stream_apply_worker](../p/pa_set_stream_apply_worker.md)
  - [pa_lock_transaction](../p/pa_lock_transaction.md)
  - [pa_set_xact_state](../p/pa_set_xact_state.md)
  - [logicalrep_worker_wakeup](../l/logicalrep_worker_wakeup.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
- Called from:
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- Sets the global variable  to true to indicate streaming mode
- Stores the transaction XID in the global  variable
- Uses error context setting for better error reporting during streaming transactions
- Implements sophisticated parallel processing logic to maximize replication throughput
- The function must handle the transition between different processing modes gracefully
- Includes proper locking mechanisms to coordinate between leader and parallel workers

## Simplified Source

```c
static void apply_handle_stream_start(StringInfo s)
{
    bool first_segment;
    ParallelApplyWorkerInfo *winfo;
    TransApplyAction apply_action;

    // Save original message for potential serialization
    StringInfoData original_msg = *s;

    // Validate: no duplicate STREAM START messages
    if (in_streamed_transaction)
        ereport(ERROR, "duplicate STREAM START message");

    // Mark that we're processing a streaming transaction
    in_streamed_transaction = true;

    // Extract transaction ID from message
    stream_xid = logicalrep_read_stream_start(s, &first_segment);

    // Validate transaction ID
    if (!TransactionIdIsValid(stream_xid))
        ereport(ERROR, "invalid transaction ID in streamed replication");

    set_apply_error_context_xact(stream_xid, InvalidXLogRecPtr);

    // Try to allocate parallel worker for first segment
    if (first_segment)
        pa_allocate_worker(stream_xid);

    // Determine how to handle this transaction
    apply_action = get_transaction_apply_action(stream_xid, &winfo);

    switch (apply_action) {
        case TRANS_LEADER_SERIALIZE:
            // Serialize changes to spool file
            stream_start_internal(stream_xid, first_segment);
            break;

        case TRANS_LEADER_SEND_TO_PARALLEL:
            // Send data directly to parallel worker
            if (pa_send_data(winfo, s->len, s->data)) {
                if (!first_segment)
                    pa_unlock_stream(winfo->shared->xid, AccessExclusiveLock);

                pg_atomic_add_fetch_u32(&winfo->shared->pending_stream_count, 1);
                pa_set_stream_apply_worker(winfo);
                break;
            }
            // Fall through to partial serialize if sending fails
            pa_switch_to_partial_serialize(winfo, !first_segment);

        case TRANS_LEADER_PARTIAL_SERIALIZE:
            // Fallback: serialize after parallel attempt failed
            if (apply_action != TRANS_LEADER_SEND_TO_PARALLEL)
                stream_start_internal(stream_xid, first_segment);

            stream_write_change(LOGICAL_REP_MSG_STREAM_START, &original_msg);
            pa_set_stream_apply_worker(winfo);
            break;

        case TRANS_PARALLEL_APPLY:
            // Handle in parallel apply worker
            if (first_segment) {
                pa_lock_transaction(MyParallelShared->xid, AccessExclusiveLock);
                pa_set_xact_state(MyParallelShared, PARALLEL_TRANS_STARTED);
                logicalrep_worker_wakeup(MyLogicalRepWorker->subid, InvalidOid);
            }
            parallel_stream_nchanges = 0;
            break;

        default:
            elog(ERROR, "unexpected apply action: %d", (int) apply_action);
    }

    pgstat_report_activity(STATE_RUNNING, NULL);
}
```