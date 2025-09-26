# handle_streamed_transaction

## Location
[src/backend/replication/logical/worker.c:561-653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L561-L653)

## Overview
Manages streamed transactions for both leader apply workers and parallel apply workers by routing transaction changes to appropriate processing paths based on the transaction apply mode.

## Definition
```c
static bool handle_streamed_transaction(LogicalRepMsgType action, StringInfo s)
```

## Detailed Description
This function implements the core logic for handling streaming transaction mode in logical replication. It determines how to process streaming transaction changes based on the current transaction apply action:

**TRANS_LEADER_APPLY**: Regular non-streaming mode - returns false to process normally

**TRANS_LEADER_SERIALIZE**: Serializes changes to disk files for later processing, adding subxact information and writing changes to the stream file

**TRANS_LEADER_SEND_TO_PARALLEL**: Attempts to send changes to parallel apply workers via inter-process communication. Falls back to partial serialization if sending fails

**TRANS_LEADER_PARTIAL_SERIALIZE**: Writes changes to disk when parallel sending is unavailable

**TRANS_PARALLEL_APPLY**: Processes changes in parallel apply worker context, managing subtransactions and savepoints

The function includes special handling for RELATION and TYPE messages, ensuring they are processed by both leader and parallel workers for cache consistency.

## Parameters / Member Variables
- `action`: LogicalRepMsgType indicating the type of replication message being processed
- `s`: StringInfo containing the serialized message data

## Dependencies
- Functions called/Symbols referenced:
  - LogicalRepMsgType, ParallelApplyWorkerInfo, TransApplyAction (type definitions)
  - [get_transaction_apply_action](../g/get_transaction_apply_action.md) (determines processing mode)
  - [pq_getmsgint](../p/pq_getmsgint.md) (extracts transaction ID from message)
  - [subxact_info_add](../s/subxact_info_add.md) (tracks subxact information)
  - [stream_write_change](../s/stream_write_change.md) (writes changes to disk)
  - [pa_send_data](../p/pa_send_data.md) (sends data to parallel worker)
  - [pa_switch_to_partial_serialize](../p/pa_switch_to_partial_serialize.md) (switches to serialization mode)
  - [pa_start_subtrans](../p/pa_start_subtrans.md) (manages subtransaction savepoints)
  - TRANS_LEADER_APPLY, TRANS_LEADER_SERIALIZE, TRANS_LEADER_SEND_TO_PARALLEL, TRANS_LEADER_PARTIAL_SERIALIZE, TRANS_PARALLEL_APPLY (apply action constants)
  - LOGICAL_REP_MSG_RELATION, LOGICAL_REP_MSG_TYPE (message type constants)
- Called from (representative examples):
  - [apply_handle_insert](../a/apply_handle_insert.md) (INSERT message processing)
  - [apply_handle_update](../a/apply_handle_update.md) (UPDATE message processing)
  - [apply_handle_delete](../a/apply_handle_delete.md) (DELETE message processing)
  - [apply_handle_relation](../a/apply_handle_relation.md) (RELATION message processing)
  - [apply_handle_type](../a/apply_handle_type.md) (TYPE message processing)
  - [apply_handle_truncate](../a/apply_handle_truncate.md) (TRUNCATE message processing)

## Notes and Other Information
- Returns true when changes are handled by streaming logic (serialized or sent to parallel worker)
- Returns false when changes need normal processing or are in parallel apply worker context
- Special exception: RELATION and TYPE messages return false even when sent to parallel workers to ensure leader processing
- Includes fallback mechanism from parallel sending to partial serialization for reliability
- Validates transaction IDs and reports protocol violations for invalid XIDs
- Part of PostgreSQL's advanced logical replication streaming and parallel processing infrastructure
- Handles both streaming serialization for large transactions and parallel processing for performance