# pltcl_func_handler

## Location
src/pl/tcl/pltcl.c: 797 - 1055

## Overview
Handles regular function calls for PL/Tcl, managing argument conversion, Tcl function execution, and result processing for both scalar and set-returning functions.

## Definition
```c
static Datum pltcl_func_handler(PG_FUNCTION_ARGS, pltcl_call_state *call_state, bool pltrusted)
```

## Detailed Description
`pltcl_func_handler` is the core function execution handler for PL/Tcl that processes regular (non-trigger) function calls. It performs the complete lifecycle of function execution: establishing SPI connection, compiling/finding the function, converting PostgreSQL arguments to Tcl format, executing the Tcl function, and converting results back to PostgreSQL format.

The function handles multiple return types including scalars, tuples, and set-returning functions. It manages proper memory contexts, reference counting, and exception handling throughout the execution process. For set-returning functions, it supports materialized tuple stores. For composite types, it handles both named composite types and RECORD types with dynamic structure determination.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing arguments and call context
- `call_state`: Pointer to pltcl_call_state structure tracking execution state and resources
- `pltrusted`: Boolean flag indicating whether to operate in trusted (true) or untrusted (false) mode

## Dependencies
- Functions called/Symbols referenced:
  - SPI_connect_ext/SPI_finish (SPI interface management)
  - compile_pltcl_function (function compilation/lookup)
  - pltcl_build_tuple_argument (tuple to Tcl conversion)
  - pltcl_build_tuple_result (Tcl to tuple conversion)
  - OutputFunctionCall/InputFunctionCall (data type I/O)
  - HeapTupleHeader functions (tuple manipulation)
  - TupleDescGetAttInMetadata (tuple descriptor utilities)
  - CreateTupleDescCopy (tuple descriptor copying)
  - throw_tcl_error (error handling)
  - Tcl library functions (Tcl_EvalObjEx, Tcl_ListObjAppendElement, etc.)
- Called from (representative examples):
  - pltcl_handler (main dispatcher)

## Notes and Other Information
- This is a static function, not directly accessible outside the PL/Tcl module
- Supports both atomic and non-atomic execution contexts
- Handles NULL arguments and return values appropriately
- Implements proper UTF-8 encoding conversion between PostgreSQL and Tcl
- Manages reference counting for Tcl objects to prevent memory leaks
- Supports complex argument types including row types and domains
- For set-returning functions, uses materialized tuple stores in the caller's memory context
- Implements comprehensive error handling with proper resource cleanup
- Handles both predetermined composite return types and dynamic RECORD types