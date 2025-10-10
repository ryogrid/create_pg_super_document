# pltcl_handler

## Location
[src/pl/tcl/pltcl.c:723-796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L723-L796)

## Overview
Core handler function for PL/Tcl that routes function calls, triggers, and event triggers to appropriate subhandlers for both trusted and untrusted Tcl interpreters.

## Definition
```c
static Datum pltcl_handler(PG_FUNCTION_ARGS, bool pltrusted)
```

## Detailed Description
`pltcl_handler` is the central dispatch function for the PL/Tcl procedural language handler in PostgreSQL. It serves as the main routing mechanism that determines the type of call (function, trigger, or event trigger) and delegates execution to the appropriate specialized subhandler. The function manages call state, reference counting for procedure descriptors, and ensures proper cleanup through PostgreSQL's exception handling framework.

The function establishes a call state context that tracks the current execution environment, manages procedure descriptor lifecycles through reference counting, and provides proper cleanup even in error conditions. It operates in both trusted and untrusted modes based on the pltrusted parameter.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing all necessary context for function execution
- `pltrusted`: Boolean flag indicating whether to operate in trusted (true) or untrusted (false) mode

## Dependencies
- Functions called/Symbols referenced:
  - [pltcl_call_state](pltcl_call_state.md) (call state structure)
  - PG_TRY/PG_FINALLY/PG_END_TRY (exception handling macros)
  - CALLED_AS_TRIGGER (macro to detect trigger calls)
  - CALLED_AS_EVENT_TRIGGER (macro to detect event trigger calls)
  - [pltcl_trigger_handler](pltcl_trigger_handler.md) (trigger execution handler)
  - [pltcl_event_trigger_handler](pltcl_event_trigger_handler.md) (event trigger execution handler)
  - [pltcl_func_handler](pltcl_func_handler.md) (function execution handler)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (memory management)
- Called from (representative examples):
  - [pltcl_call_handler](pltcl_call_handler.md) (trusted entry point)
  - [pltclu_call_handler](pltclu_call_handler.md) (untrusted entry point)

## Notes and Other Information
- This is a static function, not directly accessible outside the PL/Tcl module
- Implements proper reference counting for procedure descriptors to handle concurrent access and replacement
- Uses PostgreSQL's PG_TRY/PG_FINALLY exception handling to ensure cleanup in error conditions
- Manages the global pltcl_current_call_state pointer for nested call support
- Supports three types of invocations: regular functions, triggers, and event triggers
- The call state tracking ensures proper resource management and supports re-entrant calls

## Simplified Source

```c
static Datum
pltcl_handler(PG_FUNCTION_ARGS, bool pltrusted)
{
    Datum retval = (Datum) 0;
    pltcl_call_state current_call_state;
    pltcl_call_state *save_call_state;

    // Initialize call state and save previous state
    memset(&current_call_state, 0, sizeof(current_call_state));
    save_call_state = pltcl_current_call_state;
    pltcl_current_call_state = &current_call_state;

    PG_TRY();
    {
        // Determine call type and dispatch to appropriate handler
        if (CALLED_AS_TRIGGER(fcinfo)) {
            // Handle trigger calls
            retval = PointerGetDatum(pltcl_trigger_handler(fcinfo,
                                                          &current_call_state,
                                                          pltrusted));
        }
        else if (CALLED_AS_EVENT_TRIGGER(fcinfo)) {
            // Handle event trigger calls
            pltcl_event_trigger_handler(fcinfo, &current_call_state, pltrusted);
            retval = (Datum) 0;
        }
        else {
            // Handle regular function calls
            current_call_state.fcinfo = fcinfo;
            retval = pltcl_func_handler(fcinfo, &current_call_state, pltrusted);
        }
    }
    PG_FINALLY();
    {
        // Restore previous state and clean up procedure descriptor
        pltcl_current_call_state = save_call_state;
        if (current_call_state.prodesc != NULL) {
            Assert(current_call_state.prodesc->fn_refcount > 0);
            if (--current_call_state.prodesc->fn_refcount == 0)
                MemoryContextDelete(current_call_state.prodesc->fn_cxt);
        }
    }
    PG_END_TRY();

    return retval;
}
```