# LogicalErrorCallbackState

## Location
src/backend/replication/logical/logical.c: 50 - 55

## Overview
LogicalErrorCallbackState is a structure used to maintain context information for error reporting during logical replication callback execution, providing detailed error messages that include slot name, plugin name, callback type, and LSN location.

## Definition
```c
typedef struct LogicalErrorCallbackState
{
    LogicalDecodingContext *ctx;
    const char *callback_name;
    XLogRecPtr  report_location;
} LogicalErrorCallbackState;
```

## Detailed Description
This structure serves as a data container for PostgreSQL's error context callback mechanism specifically within the logical replication subsystem. When logical replication callbacks (such as begin_cb, commit_cb, change_cb, etc.) are executed, this structure is used to track contextual information needed for meaningful error reporting.

The structure is primarily used in conjunction with the `output_plugin_error_callback` function, which formats error messages that include the replication slot name, output plugin name, the specific callback being executed, and optionally the associated LSN (Log Sequence Number) when available.

Each logical replication callback wrapper function creates a local instance of this structure, populates it with relevant context information, and registers it with PostgreSQL's error context stack before calling the actual plugin callback. This ensures that if an error occurs during callback execution, users receive detailed diagnostic information about where and when the error occurred.

## Parameters / Member Variables
- `ctx`: Pointer to the LogicalDecodingContext containing the overall context for logical decoding, including slot information and callback functions
- `callback_name`: String identifier of the specific callback being executed (e.g., "begin", "commit", "change", "truncate")
- `report_location`: XLogRecPtr representing the LSN (Log Sequence Number) associated with the callback operation, or InvalidXLogRecPtr if no specific location applies

## Dependencies
- Functions called/Symbols referenced:
  - LogicalDecodingContext (struct type for ctx member)
  - XLogRecPtr (type for report_location member)
- Called from (representative examples):
  - output_plugin_error_callback (uses this structure for error context)
  - begin_cb_wrapper (creates and populates instance)
  - commit_cb_wrapper (creates and populates instance)
  - change_cb_wrapper (creates and populates instance)
  - All other logical replication callback wrappers

## Notes and Other Information
- This structure is defined in src/backend/replication/logical/logical.c:50-55 as a private implementation detail
- Used exclusively for error reporting and context tracking, not for actual logical replication data processing
- The structure is typically allocated on the stack within callback wrapper functions
- The error callback mechanism helps administrators debug logical replication issues by providing precise context about where errors occur
- The report_location field may be InvalidXLogRecPtr for callbacks that don't have an associated specific LSN
- Part of PostgreSQL's comprehensive error handling infrastructure for logical replication operations