# plperl_call_handler

## Location
[src/pl/plperl/plperl.c:1852-1893](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1852-L1893)

## Overview
Main entry point for PL/Perl function calls that dispatches to appropriate handlers based on the calling context (regular function, trigger, or event trigger).

## Definition


## Detailed Description
This function serves as the primary dispatcher for all PL/Perl function invocations within PostgreSQL. It determines the type of call (regular function, trigger, or event trigger) and routes execution to the appropriate specialized handler. The function implements proper exception handling using PostgreSQL's PG_TRY/PG_FINALLY mechanism to ensure cleanup of interpreter state and reference counting even when errors occur. It maintains call data state and manages Perl interpreter activation/deactivation around the actual function execution.

## Parameters / Member Variables
- Implicit  parameter (via PG_FUNCTION_ARGS macro): PostgreSQL function call information containing all necessary context

## Dependencies
- Functions called/Symbols referenced:
  - [plperl_call_data](plperl_call_data.md) (structure for call state management)
  - [plperl_interp_desc](plperl_interp_desc.md) (Perl interpreter descriptor)
  - MemSet (memory initialization)
  - CALLED_AS_TRIGGER (macro to detect trigger context)
  - CALLED_AS_EVENT_TRIGGER (macro to detect event trigger context)
  - [plperl_trigger_handler](plperl_trigger_handler.md) (handler for regular triggers)
  - [plperl_event_trigger_handler](plperl_event_trigger_handler.md) (handler for event triggers)
  - [plperl_func_handler](plperl_func_handler.md) (handler for regular functions)
  - [activate_interpreter](../a/activate_interpreter.md) (Perl interpreter management)
  - decrement_prodesc_refcount (reference counting for function descriptors)
  - PG_TRY, PG_FINALLY, PG_END_TRY (PostgreSQL exception handling)
- Called from (representative examples):
  - [plperlu_call_handler](plperlu_call_handler.md)

## Notes and Other Information
- Uses PostgreSQL's structured exception handling to ensure proper cleanup
- Maintains current_call_data global state during execution
- Handles interpreter switching for different Perl execution contexts
- Implements reference counting for function procedure descriptors
- Returns appropriate Datum values for different call types (event triggers return 0)
- Central routing point for all PL/Perl function invocations in PostgreSQL
- Must be declared with PG_FUNCTION_INFO_V1 for PostgreSQL function interface