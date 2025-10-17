# apply_handle_begin

## Location
[src/backend/replication/logical/worker.c:993-1017](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L993-L1017)

## Overview
apply_handle_begin handles BEGIN messages in PostgreSQL logical replication, marking the start of a remote transaction on the apply worker side.

## Definition
```c
static void apply_handle_begin(StringInfo s)
```

## Detailed Description
This function processes logical replication BEGIN messages that signal the start of a new transaction from the publisher. It reads the transaction details from the message stream, establishes error context for proper error reporting, sets up transaction state tracking, and optionally initiates change skipping based on LSN filtering. The function ensures that no streaming transaction is currently active and prepares the apply worker to process subsequent changes within this transaction context.

## Parameters / Member Variables
- `s`: StringInfo containing the serialized BEGIN message data from the logical replication stream

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalRepBeginData](../L/LogicalRepBeginData.md) (struct for storing begin message data)
  - [logicalrep_read_begin](../l/logicalrep_read_begin.md) (deserializes BEGIN message from stream)
  - [set_apply_error_context_xact](../s/set_apply_error_context_xact.md) (establishes error context for transaction)
  - [maybe_start_skipping_changes](../m/maybe_start_skipping_changes.md) (initiates LSN-based change filtering if needed)
  - [pgstat_report_activity](../p/pgstat_report_activity.md) (reports worker activity status)
  - STATE_RUNNING (activity state constant)
- Called from:
  - [apply_dispatch](apply_dispatch.md) (main message dispatcher for logical replication)

## Notes and Other Information
- This function asserts that no streaming transaction is currently active (stream_xid must be invalid)
- Sets the global flag in_remote_transaction to true to track transaction state
- Stores the final_lsn for the transaction in the global remote_final_lsn variable
- The function is static and only called internally within the logical replication worker
- Part of PostgreSQL's logical replication apply worker message handling system

## Simplified Source

```c
static void
apply_handle_begin(StringInfo s)
{
    LogicalRepBeginData begin_data;

    // No streaming transaction should be active
    Assert(!TransactionIdIsValid(stream_xid));

    // Read transaction begin data from message
    logicalrep_read_begin(s, &begin_data);
    set_apply_error_context_xact(begin_data.xid, begin_data.final_lsn);

    // Store transaction final LSN and start tracking
    remote_final_lsn = begin_data.final_lsn;
    maybe_start_skipping_changes(begin_data.final_lsn);
    in_remote_transaction = true;

    // Report worker is now processing transaction
    pgstat_report_activity(STATE_RUNNING, NULL);
}
```