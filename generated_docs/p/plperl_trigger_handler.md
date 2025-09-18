# plperl_trigger_handler

## Location
src/pl/plperl/plperl.c: 2521 - 2633

## Overview
This function serves as the main handler for PL/Perl trigger functions, managing trigger execution, result processing, and row modification based on trigger return values.

## Definition
```c
static Datum plperl_trigger_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
`plperl_trigger_handler` is the primary entry point for executing PL/Perl trigger functions in PostgreSQL. It manages the complete trigger execution lifecycle including SPI connection with transition table registration, trigger data preparation, function compilation and execution, and result interpretation. The function handles the PostgreSQL trigger protocol where return values control trigger behavior: undef (proceed with original operation), "SKIP" (cancel operation), or "MODIFY" (use modified row data).

The function builds the `$_TD` hash containing trigger metadata and row data, executes the Perl trigger function, and processes the return value to determine the appropriate action. For row modification, it converts the Perl hash back to a PostgreSQL HeapTuple.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro providing access to:
  - `fcinfo`: FunctionCallInfo containing trigger context and metadata
  - `fcinfo->context`: Cast to TriggerData containing trigger-specific information

## Dependencies
- Functions called/Symbols referenced:
  - SPI_connect / SPI_finish (SPI connection management)
  - SPI_register_trigger_data (transition table support)
  - compile_plperl_function (function compilation)
  - increment_prodesc_refcount (reference counting)
  - activate_interpreter (Perl interpreter management)
  - plperl_trigger_build_args (builds $_TD hash)
  - plperl_call_perl_trigger_func (executes Perl code)
  - plperl_modify_tuple (converts Perl hash to HeapTuple)
  - sv2cstr (string conversion)
  - plperl_exec_callback (error context)
- Called from:
  - plperl_call_handler

## Notes and Other Information
- Registers transition tables with SPI for NEW/OLD table access in trigger functions
- Supports all trigger types: INSERT, UPDATE, DELETE, TRUNCATE
- Implements PostgreSQL trigger protocol with three return value interpretations:
  - undef/NULL: proceed with original tuple
  - "SKIP": cancel the triggering operation
  - "MODIFY": use modified row data from $_TD hash
- Handles row modification only for INSERT and UPDATE triggers (warns for DELETE)
- Provides comprehensive error handling with meaningful error messages
- Manages Perl reference counting for proper memory cleanup
- Uses error context callbacks for better error reporting with function names
- Validates trigger return values and provides clear error messages for invalid returns