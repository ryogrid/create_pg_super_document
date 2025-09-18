# per_MultiFuncCall

## Location
[src/backend/utils/fmgr/funcapi.c:208-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L208-L219)

## Overview
per_MultiFuncCall retrieves the previously initialized FuncCallContext for use during each call of a set-returning function.

## Definition
```c
FuncCallContext *per_MultiFuncCall(PG_FUNCTION_ARGS)
```

## Detailed Description
This is a simple accessor function that forms part of the standard PostgreSQL set-returning function (SRF) protocol. It retrieves the FuncCallContext structure that was previously created and stored by `init_MultiFuncCall()`. 

The function assumes that initialization has already been completed and simply returns the context pointer that was stored in `fcinfo->flinfo->fn_extra`. This lightweight operation allows set-returning functions to quickly access their persistent state data on each subsequent call after initialization.

This function is typically called on every invocation of a set-returning function (after the first) to access the function's cross-call state information, including call counters, user data, tuple descriptors, and other persistent information.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `fcinfo`: Function call information structure containing the stored FuncCallContext

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple accessor function)
- Types referenced:
  - [FuncCallContext](../F/FuncCallContext.md)
- Called from (representative examples):
  - SRF_PERCALL_SETUP (macro)
  - Various set-returning functions during their per-call processing

## Notes and Other Information
- This function assumes `init_MultiFuncCall()` has been previously called to initialize the context
- No validation is performed - the function assumes the context exists and is valid
- This is typically called through the SRF_PERCALL_SETUP() macro rather than directly
- The function is extremely lightweight, consisting of only a single pointer dereference and return
- Part of the standard SRF (Set Returning Function) protocol alongside init_MultiFuncCall and end_MultiFuncCall
- The returned context contains state information that persists across multiple function calls