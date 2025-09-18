# init_MultiFuncCall

## Location
src/backend/utils/fmgr/funcapi.c: 133 - 207

## Overview
init_MultiFuncCall creates and initializes a FuncCallContext data structure for set-returning functions, providing the foundational setup for multi-call function execution.

## Definition
```c
FuncCallContext *init_MultiFuncCall(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the standard initialization routine for set-returning functions that need to maintain state across multiple calls. It implements the SRF (Set Returning Function) protocol by:

1. **Context Validation**: Verifies the function is called in an appropriate context that can handle set-returning results
2. **First-Call Detection**: Checks if this is the first call by examining `fcinfo->flinfo->fn_extra`
3. **Memory Context Creation**: Establishes a dedicated memory context for cross-call data that will persist across function invocations
4. **Structure Initialization**: Allocates and initializes a FuncCallContext structure with default values
5. **Cleanup Registration**: Registers a callback to ensure proper cleanup if the expression context terminates unexpectedly

The function enforces that it can only be called once per function invocation sequence, raising an error on subsequent initialization attempts.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `fcinfo`: Function call information structure
  - Function parameters and context

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - MemoryContextAllocZero
  - RegisterExprContextCallback
  - shutdown_MultiFuncCall
- Types referenced:
  - FuncCallContext
  - ReturnSetInfo
- Constants used:
  - ALLOCSET_SMALL_SIZES
- Called from (representative examples):
  - SRF_FIRSTCALL_INIT (macro)
  - Various set-returning functions that use the standard SRF protocol

## Notes and Other Information
- This function can only be called once per function invocation sequence - subsequent calls will raise an ERROR
- The created memory context persists across all calls to the set-returning function
- A cleanup callback is registered to prevent memory leaks if the expression context is interrupted
- The function initializes the FuncCallContext with sensible defaults (call_cntr=0, max_calls=0, etc.)
- This is typically called through the SRF_FIRSTCALL_INIT() macro rather than directly
- The function pointer is stored in `fcinfo->flinfo->fn_extra` to detect repeated initialization attempts
- Memory allocation uses ALLOCSET_SMALL_SIZES for efficiency with typical SRF usage patterns