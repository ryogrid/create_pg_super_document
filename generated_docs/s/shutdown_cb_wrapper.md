# shutdown_cb_wrapper

## Location
src/backend/replication/logical/logical.c: 821 - 853

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
  - LogicalDecodingContext
  - LogicalErrorCallbackState  
  - output_plugin_error_callback
  - callback
- Called from (representative examples):
  - FreeDecodingContext

## Notes and Other Information
- The function asserts that fast_forward mode is disabled during shutdown
- Sets ctx->accept_writes and ctx->end_xact to false before calling the plugin
- Manages error context stack to provide detailed error information on failures
- The error callback has no associated LSN since shutdown operations don't correspond to specific WAL positions
- Static function used internally within logical replication infrastructure
- Called during cleanup when freeing decoding contexts
- Part of the plugin callback wrapper ecosystem that provides consistent error handling across all plugin operations
- Critical for proper plugin resource cleanup and error reporting during logical replication termination