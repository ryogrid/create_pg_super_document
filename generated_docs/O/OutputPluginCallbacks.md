# OutputPluginCallbacks

## Location
[src/include/replication/output_plugin.h:216-243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/output_plugin.h#L216-L243)

## Overview
OutputPluginCallbacks is a structure that defines the complete set of callback functions that logical replication output plugins must implement to handle various events during logical decoding.

## Definition
```c
typedef struct OutputPluginCallbacks
{
    LogicalDecodeStartupCB startup_cb;
    LogicalDecodeBeginCB begin_cb;
    LogicalDecodeChangeCB change_cb;
    LogicalDecodeTruncateCB truncate_cb;
    LogicalDecodeCommitCB commit_cb;
    LogicalDecodeMessageCB message_cb;
    LogicalDecodeFilterByOriginCB filter_by_origin_cb;
    LogicalDecodeShutdownCB shutdown_cb;

    /* streaming of changes at prepare time */
    LogicalDecodeFilterPrepareCB filter_prepare_cb;
    LogicalDecodeBeginPrepareCB begin_prepare_cb;
    LogicalDecodePrepareCB prepare_cb;
    LogicalDecodeCommitPreparedCB commit_prepared_cb;
    LogicalDecodeRollbackPreparedCB rollback_prepared_cb;

    /* streaming of changes */
    LogicalDecodeStreamStartCB stream_start_cb;
    LogicalDecodeStreamStopCB stream_stop_cb;
    LogicalDecodeStreamAbortCB stream_abort_cb;
    LogicalDecodeStreamPrepareCB stream_prepare_cb;
    LogicalDecodeStreamCommitCB stream_commit_cb;
    LogicalDecodeStreamChangeCB stream_change_cb;
    LogicalDecodeStreamMessageCB stream_message_cb;
    LogicalDecodeStreamTruncateCB stream_truncate_cb;
} OutputPluginCallbacks;
```

## Detailed Description
OutputPluginCallbacks serves as the interface contract between PostgreSQL's logical decoding infrastructure and output plugins. This structure contains function pointers to all the callback functions that an output plugin can implement to receive notifications about various database events during logical replication.

The callbacks are organized into three main categories: basic transaction callbacks for handling standard transactions, prepared transaction callbacks for two-phase commit support, and streaming callbacks for handling large transactions that are streamed incrementally to avoid memory issues.

Output plugins populate this structure during their initialization (_PG_output_plugin_init function) to register their handlers for different types of events. Not all callbacks are mandatory - plugins can set unused callbacks to NULL.

## Parameters / Member Variables

### Basic Transaction Callbacks
- `startup_cb`: Called when the decoding session starts, used to initialize plugin state and set OutputPluginOptions
- `begin_cb`: Called at the beginning of each transaction
- `change_cb`: Called for each individual change (INSERT/UPDATE/DELETE) within a transaction
- `truncate_cb`: Called for TRUNCATE operations within a transaction
- `commit_cb`: Called when a transaction commits
- `message_cb`: Called for generic logical decoding messages
- `filter_by_origin_cb`: Called to filter changes based on their replication origin
- `shutdown_cb`: Called when the decoding session ends

### Prepared Transaction Callbacks
- `filter_prepare_cb`: Called to decide whether to decode a prepared transaction immediately or wait for commit
- `begin_prepare_cb`: Called at the beginning of a prepared transaction
- `prepare_cb`: Called when a transaction is prepared (PREPARE TRANSACTION)
- `commit_prepared_cb`: Called when a prepared transaction is committed (COMMIT PREPARED)
- `rollback_prepared_cb`: Called when a prepared transaction is rolled back (ROLLBACK PREPARED)

### Streaming Callbacks
- `stream_start_cb`: Called when starting to stream changes from a large in-progress transaction
- `stream_stop_cb`: Called when stopping streaming of changes from an in-progress transaction
- `stream_abort_cb`: Called when a streamed transaction is aborted
- `stream_prepare_cb`: Called when a streamed transaction is prepared
- `stream_commit_cb`: Called when a streamed transaction is committed
- `stream_change_cb`: Called for individual changes within a streamed transaction
- `stream_message_cb`: Called for messages within a streamed transaction
- `stream_truncate_cb`: Called for truncate operations within a streamed transaction

## Dependencies
- Functions called/Symbols referenced:
  - Various LogicalDecode*CB callback type definitions
- Called from (representative examples):
  - [LoadOutputPlugin](../L/LoadOutputPlugin.md) (src/backend/replication/logical/logical.c:752)
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (src/backend/replication/pgoutput/pgoutput.c:254)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (src/include/replication/logical.h:53)

## Notes and Other Information
This structure is the core interface for logical replication output plugins in PostgreSQL. The streaming callbacks are particularly important for handling large transactions that cannot fit entirely in memory. Output plugins must implement at least the basic callbacks (startup_cb, begin_cb, change_cb, commit_cb) to function properly. The prepared transaction and streaming callbacks are optional and can be set to NULL if not needed. The structure is populated during plugin initialization and remains constant throughout the decoding session.