# pltclu_call_handler

## Location
src/pl/tcl/pltcl.c: 712 - 722

## Overview
Entry point function for the PL/Tcl untrusted language handler that processes function calls in the untrusted Tcl environment.

## Definition
```c
Datum pltclu_call_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
`pltclu_call_handler` serves as the main entry point for the PL/Tcl untrusted procedural language handler in PostgreSQL. This function acts as a thin wrapper that delegates all actual processing to the `pltcl_handler` function, specifically configuring it to operate in untrusted mode (indicated by the `false` parameter). The function follows PostgreSQL's standard function calling convention using the `PG_FUNCTION_ARGS` macro.

This handler is responsible for executing Tcl functions that have been defined within the database and called from SQL statements. The untrusted nature means it operates with elevated privileges and broader access to system resources, making it suitable for administrative tasks but requiring careful security considerations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing all necessary context for function execution, including arguments and call metadata

## Dependencies
- Functions called/Symbols referenced:
  - [pltcl_handler](pltcl_handler.md) (the core handler function)
- Called from (representative examples):
  - [pltcl_call_handler](pltcl_call_handler.md) (cross-reference in function info)

## Notes and Other Information
- This function is marked as "keep non-static" indicating it needs external visibility
- It specifically handles untrusted Tcl execution (as opposed to trusted)
- The function acts purely as an entry point wrapper with no additional logic
- Part of the PL/Tcl procedural language extension for PostgreSQL
- Untrusted mode allows broader system access but requires superuser privileges to create functions