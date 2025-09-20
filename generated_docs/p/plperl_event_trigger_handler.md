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
  - Various macros for accessing function parameters and return values

## Dependencies
- Functions called/Symbols referenced:
  - SPI_connect: Establishes connection to SPI manager
  - [compile_plperl_function](../c/compile_plperl_function.md): Compiles or retrieves cached function descriptor
  - increment_prodesc_refcount: Increments reference count for function descriptor
  - [plperl_exec_callback](plperl_exec_callback.md): Error callback function for PL/Perl execution
  - [activate_interpreter](../a/activate_interpreter.md): Activates the appropriate Perl interpreter
  - [plperl_event_trigger_build_args](plperl_event_trigger_build_args.md): Builds argument structure for event trigger
  - [plperl_call_perl_event_trigger_func](plperl_call_perl_event_trigger_func.md): Calls the actual Perl event trigger function
  - SPI_finish: Cleans up SPI connection
  - [SvREFCNT_dec_current](../S/SvREFCNT_dec_current.md): Decrements Perl scalar reference count
- Called from:
  - [plperl_call_handler](plperl_call_handler.md): Main PL/Perl function dispatcher

## Notes and Other Information
- This function is specific to event triggers and differs from regular function handlers
- Uses proper error context management to provide meaningful error messages
- Manages Perl interpreter lifecycle and memory management
- Ensures SPI connection is properly established and cleaned up
- Located at src/pl/plperl/plperl.c:2634-2670