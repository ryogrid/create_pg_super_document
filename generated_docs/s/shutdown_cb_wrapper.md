# shutdown_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:821-853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L821-L853)

## Overview
shutdown_cb_wrapper is a static wrapper function that safely calls the output plugin's shutdown callback with proper error handling and context management during plugin cleanup.

## Definition
```c
static void shutdown_cb_wrapper(LogicalDecodingContext *ctx)
```

## Detailed Description
This function serves as a wrapper around output plugin shutdown callbacks, providing essential error context tracking and state management during plugin cleanup operations. It establishes an error context stack that enables detailed error reporting if the plugin's shutdown callback fails. The function manages the logical decoding context state by setting appropriate flags before invoking the actual plugin shutdown callback.

The wrapper ensures that if an error occurs during plugin shutdown, administrators receive detailed contextual information about which plugin and callback failed. It also enforces that the logical decoding context is not in fast-forward mode during shutdown operations, maintaining consistency with other callback operations.

## Parameters / Member Variables
- `ctx`: Pointer to LogicalDecodingContext containing the decoding state and plugin callbacks to be shut down

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)  
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
  - [callback](../c/callback.md)
- Called from (representative examples):
  - [FreeDecodingContext](../F/FreeDecodingContext.md)

## Notes and Other Information
- The function asserts that fast_forward mode is disabled during shutdown
- Sets ctx->accept_writes and ctx->end_xact to false before calling the plugin
- Manages error context stack to provide detailed error information on failures
- The error callback has no associated LSN since shutdown operations don't correspond to specific WAL positions
- Static function used internally within logical replication infrastructure
- Called during cleanup when freeing decoding contexts
- Part of the plugin callback wrapper ecosystem that provides consistent error handling across all plugin operations
- Critical for proper plugin resource cleanup and error reporting during logical replication termination

## Simplified Source

```c
// Simplified version of shutdown_cb_wrapper
static void shutdown_cb_wrapper(LogicalDecodingContext *ctx) {
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);

    // Set up error context for detailed error reporting
    state.ctx = ctx;
    state.callback_name = "shutdown";
    state.report_location = InvalidXLogRecPtr;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state
    ctx->accept_writes = false;
    ctx->end_xact = false;

    // Call the actual plugin shutdown callback
    ctx->callbacks.shutdown_cb(ctx);

    // Restore error context
    error_context_stack = errcallback.previous;
}
```

Key simplifications made:
- Function is already well-structured, maintains error handling framework
- Provides safety wrapper around plugin shutdown callbacks
- Essential for proper error reporting during plugin cleanup