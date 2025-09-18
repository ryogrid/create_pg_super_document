# pltcl_handler

## Location
src/pl/tcl/pltcl.c: 723 - 796

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
  - pltcl_call_state (call state structure)
  - PG_TRY/PG_FINALLY/PG_END_TRY (exception handling macros)
  - CALLED_AS_TRIGGER (macro to detect trigger calls)
  - CALLED_AS_EVENT_TRIGGER (macro to detect event trigger calls)
  - [pltcl_trigger_handler](pltcl_trigger_handler.md) (trigger execution handler)
  - pltcl_event_trigger_handler (event trigger execution handler)
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