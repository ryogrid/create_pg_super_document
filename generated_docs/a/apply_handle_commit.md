# apply_handle_commit

## Location
[src/backend/replication/logical/worker.c:1018-1043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1018-L1043)

## Overview
apply_handle_commit handles COMMIT messages in PostgreSQL logical replication, finalizing a remote transaction on the apply worker side and processing any parallel table synchronization.

## Definition
```c
static void apply_handle_commit(StringInfo s)
```

## Detailed Description
This function processes logical replication COMMIT messages that signal the completion of a transaction from the publisher. It reads the commit details from the message stream, validates that the commit LSN matches the expected final LSN for the transaction, calls the internal commit handler to finalize the transaction, processes any tables being synchronized in parallel, and cleans up the worker state. The function includes LSN validation to detect protocol violations and ensures proper coordination with parallel table synchronization operations.

## Parameters / Member Variables
- `s`: StringInfo containing the serialized COMMIT message data from the logical replication stream

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalRepCommitData](../L/LogicalRepCommitData.md) (struct for storing commit message data)
  - [logicalrep_read_commit](../l/logicalrep_read_commit.md) (deserializes COMMIT message from stream)
  - [apply_handle_commit_internal](apply_handle_commit_internal.md) (performs the actual transaction commit)
  - [process_syncing_tables](../p/process_syncing_tables.md) (processes tables being synchronized in parallel)
  - [pgstat_report_activity](../p/pgstat_report_activity.md) (reports worker activity status)
  - STATE_IDLE (activity state constant)
  - [reset_apply_error_context_info](../r/reset_apply_error_context_info.md) (cleans up error context)
- Called from:
  - [apply_dispatch](apply_dispatch.md) (main message dispatcher for logical replication)

## Notes and Other Information
- Validates that commit_data.commit_lsn matches remote_final_lsn to detect protocol violations
- Contains a TODO comment indicating future support for tracking multiple origins
- Sets worker state to idle after commit completion
- Processes parallel table synchronization as part of commit handling
- Part of PostgreSQL's logical replication apply worker message handling system
- The function ensures proper cleanup of error context information after commit

## Simplified Source

```c
static void
apply_handle_commit(StringInfo s)
{
    LogicalRepCommitData commit_data;

    // Read commit data from message
    logicalrep_read_commit(s, &commit_data);

    // Validate LSN matches expected final LSN
    if (commit_data.commit_lsn != remote_final_lsn)
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg_internal("incorrect commit LSN %X/%X in commit message (expected %X/%X)",
                                 LSN_FORMAT_ARGS(commit_data.commit_lsn),
                                 LSN_FORMAT_ARGS(remote_final_lsn))));

    // Execute the actual commit
    apply_handle_commit_internal(&commit_data);

    // Process parallel table synchronization
    process_syncing_tables(commit_data.end_lsn);

    // Clean up and report idle state
    pgstat_report_activity(STATE_IDLE, NULL);
    reset_apply_error_context_info();
}
```