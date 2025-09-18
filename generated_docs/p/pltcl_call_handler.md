# pltcl_call_handler

## Location
src/pl/tcl/pltcl.c: 700 - 711

## Overview
Entry point function for the PL/Tcl trusted language handler that processes function calls in the trusted Tcl environment.

## Definition


## Detailed Description
 serves as the main entry point for the PL/Tcl trusted procedural language handler in PostgreSQL. This function acts as a thin wrapper that delegates all actual processing to the  function, specifically configuring it to operate in trusted mode (indicated by the  parameter). The function follows PostgreSQL's standard function calling convention using the  macro.

This handler is responsible for executing Tcl functions that have been defined within the database and called from SQL statements. The trusted nature means it operates with restricted privileges and limited access to system resources for security purposes.

## Parameters / Member Variables
- : Standard PostgreSQL function call information structure containing all necessary context for function execution, including arguments and call metadata

## Dependencies
- Functions called/Symbols referenced:
  - [pltcl_handler](pltcl_handler.md) (the core handler function)
  - PG_FUNCTION_INFO_V1 (PostgreSQL function info macro)
- Called from (representative examples):
  - [start_proc_error_callback](../s/start_proc_error_callback.md)

## Notes and Other Information
- This function is marked as "keep non-static" indicating it needs external visibility
- It specifically handles trusted Tcl execution (as opposed to untrusted)
- The function acts purely as an entry point wrapper with no additional logic
- Part of the PL/Tcl procedural language extension for PostgreSQL