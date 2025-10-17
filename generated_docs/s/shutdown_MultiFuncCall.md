# shutdown_MultiFuncCall

## Location
[src/backend/utils/fmgr/funcapi.c:238-275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L238-L275)

## Overview
shutdown_MultiFuncCall is a static cleanup function that releases all resources associated with a multi-call set-returning function, including memory contexts and function state.

## Definition
```c
static void shutdown_MultiFuncCall(Datum arg)
```

## Detailed Description
This internal function serves as the actual cleanup implementation for set-returning functions using the SRF (Set Returning Function) protocol. It performs comprehensive resource cleanup by:

1. **Context Extraction**: Converts the Datum argument back to an FmgrInfo pointer and extracts the stored FuncCallContext
2. **Unbinding**: Clears the fn_extra pointer to prevent further access to the context
3. **Memory Cleanup**: Deletes the entire multi-call memory context, which automatically frees all associated memory including the FuncCallContext itself and any user-allocated data

This function is designed to be used both as a registered callback for automatic cleanup when expression contexts terminate unexpectedly, and as the implementation called directly by `end_MultiFuncCall()` during normal function completion.

The function is static (internal) and follows the ExprContextCallbackFunction signature to work with PostgreSQL's expression context callback system.

## Parameters / Member Variables
- `arg`: Datum containing a pointer to the FmgrInfo structure, which contains the FuncCallContext to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md) (implicitly used to convert Datum to pointer)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Types referenced:
  - [FmgrInfo](../F/FmgrInfo.md)
  - [FuncCallContext](../F/FuncCallContext.md)
- Called from:
  - [init_MultiFuncCall](../i/init_MultiFuncCall.md) (registered as callback)
  - [end_MultiFuncCall](../e/end_MultiFuncCall.md) (called directly)

## Notes and Other Information
- This is a static function, not directly callable from outside funcapi.c
- The function signature matches ExprContextCallbackFunction for use as a registered callback
- Deleting the multi_call_memory_ctx automatically frees all memory allocated within that context
- The function safely handles the case where it might be called multiple times by clearing fn_extra
- Part of the resource management strategy for set-returning functions to prevent memory leaks
- Can be invoked either through normal completion (via end_MultiFuncCall) or automatically on context termination
- The memory context deletion is comprehensive - it frees the FuncCallContext and all associated user data

## Simplified Source

```c
static void shutdown_MultiFuncCall(Datum arg) {
    FmgrInfo *flinfo = (FmgrInfo *) DatumGetPointer(arg);
    FuncCallContext *funcctx = (FuncCallContext *) flinfo->fn_extra;

    // Unbind context from function info
    flinfo->fn_extra = NULL;

    // Delete memory context containing all multi-call data
    MemoryContextDelete(funcctx->multi_call_memory_ctx);
}
```