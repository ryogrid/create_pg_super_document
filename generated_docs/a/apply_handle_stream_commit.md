# apply_handle_stream_commit

## Location
[src/backend/replication/logical/worker.c:2133-2242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L2133-L2242)

## Overview
apply_handle_stream_commit handles STREAM COMMIT messages in PostgreSQL logical replication, coordinating the commit process for streaming transactions across different apply strategies including parallel processing and spooling.

## Definition
```c
static void apply_handle_stream_commit(StringInfo s)
```

## Detailed Description
This function processes STREAM COMMIT messages, which signal the end of a streaming transaction in logical replication. It reads the commit data from the message, determines the appropriate apply action based on the transaction state and parallel processing configuration, and executes the commit through one of several pathways:

1. TRANS_LEADER_APPLY: Processes spooled messages from disk and commits directly
2. TRANS_LEADER_SEND_TO_PARALLEL: Sends commit to parallel apply worker, with fallback to serialization
3. TRANS_LEADER_PARTIAL_SERIALIZE: Serializes the commit message and coordinates with parallel worker
4. TRANS_PARALLEL_APPLY: Commits in parallel worker context with proper state management

The function ensures proper cleanup, state management, and coordination between leader and parallel apply workers, including file cleanup, transaction state updates, and synchronization of parallel table operations.

## Parameters / Member Variables
- `s`: StringInfo containing the STREAM COMMIT message data to be parsed and processed

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_read_stream_commit](../l/logicalrep_read_stream_commit.md) (parses commit message)
  - [set_apply_error_context_xact](../s/set_apply_error_context_xact.md)/reset_apply_error_context_info
  - [get_transaction_apply_action](../g/get_transaction_apply_action.md) (determines processing strategy)
  - [apply_spooled_messages](apply_spooled_messages.md) (processes serialized changes)
  - [apply_handle_commit_internal](apply_handle_commit_internal.md) (actual commit processing)
  - [stream_cleanup_files](../s/stream_cleanup_files.md)/stream_close_file (file management)
  - pa_send_data, pa_xact_finish, pa_switch_to_partial_serialize (parallel apply coordination)
  - [stream_open_and_write_change](../s/stream_open_and_write_change.md) (serialization)
  - [pa_set_fileset_state](../p/pa_set_fileset_state.md), pa_set_xact_state, pa_unlock_transaction (state management)
  - [process_syncing_tables](../p/process_syncing_tables.md) (parallel table sync)
  - [pgstat_report_activity](../p/pgstat_report_activity.md) (status reporting)
- Called from (representative examples):
  - [apply_dispatch](apply_dispatch.md) (main message dispatcher)

## Notes and Other Information
- Static function used internally within the logical replication worker
- Validates that STREAM STOP was called before STREAM COMMIT (protocol violation check)
- Supports complex parallel processing scenarios with automatic fallback to serialization
- Handles both leader and parallel worker contexts with appropriate state management
- Includes transaction state coordination to prevent race conditions in parallel processing
- Cleans up temporary files and resets transaction state after commit completion
- Reports activity state changes for monitoring and debugging
- Part of PostgreSQL streaming replication infrastructure for handling large transactions efficiently