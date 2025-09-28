# startup_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:793-820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L793-L820)

## Overview
startup_cb_wrapper is a static wrapper function that safely calls the output plugin's startup callback with proper error handling and context management.

## Definition
```c
static void startup_cb_wrapper(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
```

## Detailed Description
This function serves as a wrapper around output plugin startup callbacks, providing essential error context tracking and state management during plugin initialization. It establishes an error context stack that enables detailed error reporting if the plugin's startup callback fails. The function also manages the logical decoding context state by setting appropriate flags before invoking the actual plugin callback.

The wrapper ensures that if an error occurs during plugin startup, administrators receive detailed contextual information about which plugin and callback failed. It also enforces that the logical decoding context is not in fast-forward mode during startup operations.

## Parameters / Member Variables
- `ctx`: Pointer to LogicalDecodingContext containing the decoding state and plugin callbacks
- `opt`: Pointer to OutputPluginOptions structure with plugin configuration options  
- `is_init`: Boolean flag indicating whether this is an initialization call

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [OutputPluginOptions](../O/OutputPluginOptions.md)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
  - [callback](../c/callback.md)
- Called from (representative examples):
  - [CreateInitDecodingContext](../C/CreateInitDecodingContext.md)
  - [CreateDecodingContext](../C/CreateDecodingContext.md)

## Notes and Other Information
- The function asserts that fast_forward mode is disabled during startup
- Sets ctx->accept_writes and ctx->end_xact to false before calling the plugin
- Manages error context stack to provide detailed error information on failures
- The error callback has no associated LSN since startup operations don't correspond to specific WAL positions
- Static function used internally within logical replication infrastructure
- Part of the plugin callback wrapper ecosystem that provides consistent error handling across all plugin operations

## Simplified Source

```c
// Simplified version of startup_cb_wrapper
static void startup_cb_wrapper(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init) {
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);

    // Set up error context for detailed error reporting
    state.ctx = ctx;
    state.callback_name = "startup";
    state.report_location = InvalidXLogRecPtr;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state
    ctx->accept_writes = false;
    ctx->end_xact = false;

    // Call the actual plugin startup callback
    ctx->callbacks.startup_cb(ctx, opt, is_init);

    // Restore error context
    error_context_stack = errcallback.previous;
}
```

Key simplifications made:
- Function is already well-structured, maintains error handling framework
- Provides safety wrapper around plugin startup callbacks
- Essential for proper error reporting during plugin initialization