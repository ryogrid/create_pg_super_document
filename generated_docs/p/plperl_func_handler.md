# plperl_func_handler

## Location
src/pl/plperl/plperl.c: 2402 - 2520

## Overview
This function serves as the main entry point for executing PL/Perl functions in PostgreSQL, handling function compilation, execution, and result conversion.

## Definition
```c
static Datum plperl_func_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
`plperl_func_handler` is the core handler for executing PL/Perl functions within PostgreSQL. It manages the complete lifecycle of function execution including SPI connection management, function compilation, interpreter activation, execution via `plperl_call_perl_func`, and result conversion back to PostgreSQL Datum format. The function handles both scalar and set-returning functions with appropriate context validation and result processing.

For set-returning functions, it supports two modes: explicit calls to `return_next()` or returning an array reference. The function performs comprehensive error handling and maintains proper memory management throughout the execution cycle.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro which provides access to:
  - `fcinfo`: FunctionCallInfo containing function arguments and metadata
  - Function arguments accessible via `PG_GETARG_*` macros
  - Return value management via `PG_RETURN_*` macros

## Dependencies
- Functions called/Symbols referenced:
  - SPI_connect_ext / SPI_finish (SPI connection management)
  - compile_plperl_function (function compilation)
  - increment_prodesc_refcount (reference counting)
  - activate_interpreter (Perl interpreter management)
  - plperl_call_perl_func (actual Perl execution)
  - plperl_sv_to_datum (result conversion)
  - get_perl_array_ref (array handling for SRFs)
  - plperl_return_next_internal (set-returning function support)
  - plperl_exec_callback (error context)
- Called from:
  - plperl_call_handler

## Notes and Other Information
- Handles both atomic and non-atomic execution contexts appropriately
- Supports set-returning functions with comprehensive validation of execution context
- Manages SPI connections with proper cleanup to avoid memory leaks
- Implements backward compatibility for array-returning SRFs that don't use return_next()
- Uses PostgreSQL error context stack for meaningful error reporting
- Performs proper Perl reference counting with SvREFCNT_dec_current()
- Validates ReturnSetInfo context for set-returning functions
- Handles NULL returns correctly with appropriate result info management