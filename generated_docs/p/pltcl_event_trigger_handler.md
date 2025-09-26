# pltcl_event_trigger_handler

## Location
src/pl/tcl/pltcl.c: 1316 - 1370

## Overview
Handles event trigger calls for PL/Tcl procedures, setting up the Tcl environment and executing the trigger function with proper event context.

## Definition


## Detailed Description
This function serves as the event trigger handler for PL/Tcl functions. It manages the complete lifecycle of executing a Tcl event trigger procedure, including SPI connection management, function compilation/lookup, Tcl command construction, and proper error handling. The function extracts event trigger data from the function call context and passes the event name and command tag as parameters to the Tcl procedure.

The handler follows PostgreSQL's event trigger protocol by connecting to the SPI manager, compiling or finding the target Tcl function, constructing a Tcl command list with the procedure name and trigger parameters, executing the command in the Tcl interpreter, and properly cleaning up resources while handling any errors that occur during execution.

## Parameters / Member Variables
- : Standard PostgreSQL function call information structure containing function metadata and context
- : Pointer to pltcl_call_state structure for tracking call-specific state and the procedure descriptor
- : Boolean flag indicating whether this is a trusted or untrusted PL/Tcl function call

## Dependencies
- Functions called/Symbols referenced:
  - SPI_connect
  - compile_pltcl_function
  - utf_e2u
  - GetCommandTagName
  - throw_tcl_error
  - SPI_finish
- Called from (representative examples):
  - pltcl_handler

## Notes and Other Information
- Increments the function reference count to prevent premature cleanup during execution
- Uses Tcl object reference counting (Tcl_IncrRefCount/Tcl_DecrRefCount) for proper memory management
- Passes event name and command tag as UTF-8 converted strings to the Tcl procedure
- Uses TCL_EVAL_DIRECT and TCL_EVAL_GLOBAL flags for optimal Tcl command execution
- Properly handles SPI connection lifecycle with error checking on both connect and finish operations
- Event trigger data is extracted from fcinfo->context as EventTriggerData structure