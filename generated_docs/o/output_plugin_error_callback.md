# output_plugin_error_callback

## Location
[src/backend/replication/logical/logical.c:774-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L774-L792)

## Overview
output_plugin_error_callback is a static error callback function that provides detailed context information when errors occur within logical replication output plugin callbacks.

## Definition
```c
static void output_plugin_error_callback(void *arg)
```

## Detailed Description
This function serves as an error context provider for logical replication output plugin operations. When errors occur during plugin callback execution, this function is invoked to add contextual information about the specific operation that failed. It provides detailed error context including the replication slot name, plugin name, callback name, and optionally the associated LSN (Log Sequence Number) where the error occurred.

The function formats error context messages that help administrators and developers identify exactly which plugin callback failed and at what point in the replication stream, making debugging logical replication issues significantly easier.

## Parameters / Member Variables
- `arg`: Void pointer to LogicalErrorCallbackState structure containing error context information

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)
  - errcontext
- Called from (representative examples):
  - [startup_cb_wrapper](../s/startup_cb_wrapper.md)
  - [shutdown_cb_wrapper](../s/shutdown_cb_wrapper.md)
  - [begin_cb_wrapper](../b/begin_cb_wrapper.md)
  - [commit_cb_wrapper](../c/commit_cb_wrapper.md)
  - [begin_prepare_cb_wrapper](../b/begin_prepare_cb_wrapper.md)
  - [prepare_cb_wrapper](../p/prepare_cb_wrapper.md)
  - [commit_prepared_cb_wrapper](../c/commit_prepared_cb_wrapper.md)
  - [rollback_prepared_cb_wrapper](../r/rollback_prepared_cb_wrapper.md)
  - [change_cb_wrapper](../c/change_cb_wrapper.md)
  - [truncate_cb_wrapper](../t/truncate_cb_wrapper.md)
  - [filter_prepare_cb_wrapper](../f/filter_prepare_cb_wrapper.md)
  - [filter_by_origin_cb_wrapper](../f/filter_by_origin_cb_wrapper.md)
  - [message_cb_wrapper](../m/message_cb_wrapper.md)
  - [stream_start_cb_wrapper](../s/stream_start_cb_wrapper.md)
  - [stream_stop_cb_wrapper](../s/stream_stop_cb_wrapper.md)
  - [stream_abort_cb_wrapper](../s/stream_abort_cb_wrapper.md)
  - [stream_prepare_cb_wrapper](../s/stream_prepare_cb_wrapper.md)
  - [stream_commit_cb_wrapper](../s/stream_commit_cb_wrapper.md)
  - [stream_change_cb_wrapper](../s/stream_change_cb_wrapper.md)
  - [stream_message_cb_wrapper](../s/stream_message_cb_wrapper.md)
  - [stream_truncate_cb_wrapper](../s/stream_truncate_cb_wrapper.md)
  - [update_progress_txn_cb_wrapper](../u/update_progress_txn_cb_wrapper.md)

## Notes and Other Information
- The function conditionally includes LSN information in error messages when available (when report_location != InvalidXLogRecPtr)
- Error context format includes slot name, plugin name, callback name, and optionally LSN
- This callback is used extensively throughout all logical replication plugin wrapper functions
- Static function used internally within the logical replication subsystem
- Critical for debugging logical replication plugin issues in production environments

## Simplified Source

```c
static void output_plugin_error_callback(void *arg)
{
    LogicalErrorCallbackState *state = (LogicalErrorCallbackState *) arg;

    // Provide error context with LSN information if available
    if (state->report_location != InvalidXLogRecPtr)
        errcontext("slot \"%s\", output plugin \"%s\", in the %s callback, associated LSN %X/%X",
                   NameStr(state->ctx->slot->data.name),
                   NameStr(state->ctx->slot->data.plugin),
                   state->callback_name,
                   LSN_FORMAT_ARGS(state->report_location));
    else
        errcontext("slot \"%s\", output plugin \"%s\", in the %s callback",
                   NameStr(state->ctx->slot->data.name),
                   NameStr(state->ctx->slot->data.plugin),
                   state->callback_name);
}
```