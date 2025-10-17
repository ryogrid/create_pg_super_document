# end_MultiFuncCall

## Location
[src/backend/utils/fmgr/funcapi.c:220-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L220-L237)

## Overview
end_MultiFuncCall performs cleanup operations for set-returning functions, deregistering callbacks and releasing resources allocated during multi-call function execution.

## Definition
```c
void end_MultiFuncCall(PG_FUNCTION_ARGS, FuncCallContext *funcctx)
```

## Detailed Description
This function serves as the standard cleanup routine for set-returning functions that use the SRF (Set Returning Function) protocol. It completes the lifecycle of a multi-call function by:

1. **Callback Deregistration**: Removes the shutdown callback that was registered during `init_MultiFuncCall()` to prevent it from being called automatically
2. **Resource Cleanup**: Invokes the actual cleanup logic through `shutdown_MultiFuncCall()` to release memory and other resources

The two-step approach (deregister then manually invoke) ensures that cleanup happens exactly once, either through normal completion via this function or through the registered callback if the expression context terminates unexpectedly.

This function is typically called when a set-returning function has finished generating all its result rows and needs to clean up its state before returning control to the caller.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to function call information
- `funcctx`: Pointer to the FuncCallContext structure (though not directly used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [UnregisterExprContextCallback](../U/UnregisterExprContextCallback.md)
  - [shutdown_MultiFuncCall](../s/shutdown_MultiFuncCall.md)
- Types referenced:
  - [FuncCallContext](../F/FuncCallContext.md)
  - [ReturnSetInfo](../R/ReturnSetInfo.md)
- Called from (representative examples):
  - SRF_RETURN_DONE (macro)
  - Set-returning functions when they complete their result generation

## Notes and Other Information
- This function should be called exactly once per set-returning function invocation sequence
- The function deregisters the callback before manually invoking the cleanup to avoid double-cleanup
- Part of the standard SRF protocol alongside init_MultiFuncCall and per_MultiFuncCall
- Typically called through the SRF_RETURN_DONE() macro rather than directly
- The funcctx parameter is provided for consistency but not used in the current implementation
- Ensures proper memory cleanup even if the function completes normally rather than through context termination

## Simplified Source

```c
void end_MultiFuncCall(PG_FUNCTION_ARGS, FuncCallContext *funcctx) {
    ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;

    // Deregister the shutdown callback to prevent double cleanup
    UnregisterExprContextCallback(rsi->econtext,
                                  shutdown_MultiFuncCall,
                                  PointerGetDatum(fcinfo->flinfo));

    // Perform actual cleanup
    shutdown_MultiFuncCall(PointerGetDatum(fcinfo->flinfo));
}
```