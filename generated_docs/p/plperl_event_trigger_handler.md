# plperl_event_trigger_handler

## Location
[src/pl/plperl/plperl.c:2634-2670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2634-L2670)

## Overview
Handles execution of PL/Perl event trigger functions by setting up the execution environment, compiling the function if needed, and managing the call to the Perl interpreter.

## Definition

```c
static void
plperl_event_trigger_handler(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the main entry point for executing PL/Perl event trigger functions. It establishes an SPI connection, compiles or retrieves the function descriptor, sets up error handling context, activates the appropriate Perl interpreter, builds event trigger arguments, and calls the actual Perl function. The function manages the complete lifecycle of event trigger execution including proper cleanup and error handling.

## Parameters / Member Variables
- This function uses the standard PostgreSQL function calling convention  which provides access to:
  - : Function call information including function OID and arguments

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_connect](../S/SPI_connect.md): Establishes connection to SPI manager
  - [compile_plperl_function](../c/compile_plperl_function.md): Compiles or retrieves cached function descriptor
  - increment_prodesc_refcount: Increments reference count for function descriptor
  - [plperl_exec_callback](plperl_exec_callback.md): Error callback function for PL/Perl execution
  - [activate_interpreter](../a/activate_interpreter.md): Activates the appropriate Perl interpreter
  - [plperl_event_trigger_build_args](plperl_event_trigger_build_args.md): Builds argument structure for event trigger
  - [plperl_call_perl_event_trigger_func](plperl_call_perl_event_trigger_func.md): Calls the actual Perl event trigger function
  - [SPI_finish](../S/SPI_finish.md): Cleans up SPI connection
  - [SvREFCNT_dec_current](../S/SvREFCNT_dec_current.md): Decrements Perl scalar reference count
- Called from:
  - [plperl_call_handler](plperl_call_handler.md): Main PL/Perl function dispatcher

## Notes and Other Information
- This function is specific to event triggers and differs from regular function handlers
- Uses proper error context management to provide meaningful error messages
- Manages Perl interpreter lifecycle and memory management
- Ensures SPI connection is properly established and cleaned up
- Located at src/pl/plperl/plperl.c:2634-2670

## Simplified Source

```c
static void plperl_event_trigger_handler(PG_FUNCTION_ARGS) {
    plperl_proc_desc *prodesc;
    SV *svTD;
    ErrorContextCallback pl_error_context;

    // Connect to SPI manager
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "could not connect to SPI manager");

    // Find or compile the event trigger function
    prodesc = compile_plperl_function(fcinfo->flinfo->fn_oid, false, true);
    current_call_data->prodesc = prodesc;
    increment_prodesc_refcount(prodesc);

    // Set up error reporting context
    pl_error_context.callback = plperl_exec_callback;
    pl_error_context.previous = error_context_stack;
    pl_error_context.arg = prodesc->proname;
    error_context_stack = &pl_error_context;

    // Activate Perl interpreter
    activate_interpreter(prodesc->interp);

    // Build event trigger arguments and execute function
    svTD = plperl_event_trigger_build_args(fcinfo);
    plperl_call_perl_event_trigger_func(prodesc, fcinfo, svTD);

    // Cleanup SPI connection
    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish() failed");

    // Restore error context and cleanup
    error_context_stack = pl_error_context.previous;
    SvREFCNT_dec_current(svTD);
}
```