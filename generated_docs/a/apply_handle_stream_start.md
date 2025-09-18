# apply_handle_stream_start

## Location
src/backend/replication/logical/worker.c: 1469 - 1604

## Overview
Handles the STREAM START message in logical replication, initiating the processing of a streaming transaction and determining the appropriate handling strategy (serialize, parallel apply, or send to parallel worker).

## Definition


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
- : StringInfo containing the STREAM START message data to be processed

## Dependencies
- Functions called/Symbols referenced:
  - logicalrep_read_stream_start
  - set_apply_error_context_xact
  - pa_allocate_worker
  - get_transaction_apply_action
  - stream_start_internal
  - pa_send_data
  - pa_unlock_stream
  - pa_switch_to_partial_serialize
  - stream_write_change
  - pa_set_stream_apply_worker
  - pa_lock_transaction
  - pa_set_xact_state
  - logicalrep_worker_wakeup
  - pgstat_report_activity
- Called from:
  - apply_dispatch

## Notes and Other Information
- Sets the global variable  to true to indicate streaming mode
- Stores the transaction XID in the global  variable
- Uses error context setting for better error reporting during streaming transactions
- Implements sophisticated parallel processing logic to maximize replication throughput
- The function must handle the transition between different processing modes gracefully
- Includes proper locking mechanisms to coordinate between leader and parallel workers