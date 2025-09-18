# start_proc_error_callback

## Location
src/pl/tcl/pltcl.c: 680 - 699

## Overview
Error context callback function that provides enhanced error messages when problems occur during PL/Tcl startup procedure processing.

## Definition


## Detailed Description
The `start_proc_error_callback` function serves as an error context callback for PostgreSQL's error reporting system. When an error occurs during the processing of PL/Tcl startup procedures (configured via `pltcl.start_proc` or `pltclu.start_proc` GUC parameters), this callback is invoked to provide additional context information in error messages.

The function enhances error diagnostics by indicating which specific GUC parameter was being processed when the error occurred. This helps users and administrators identify whether issues are related to trusted (`pltcl.start_proc`) or untrusted (`pltclu.start_proc`) PL/Tcl startup configuration.

The callback follows PostgreSQL's standard error context callback pattern, where it receives a generic void pointer argument that it casts to the appropriate type (in this case, a GUC parameter name string).

## Parameters / Member Variables
- `arg`: Generic void pointer that contains the GUC parameter name (either "pltcl.start_proc" or "pltclu.start_proc") being processed when the error occurred

## Dependencies
- Functions called/Symbols referenced:
  - `errcontext` (adds context information to PostgreSQL error reports)
- Called from (representative examples):
  - [call_pltcl_start_proc](../c/call_pltcl_start_proc.md) (set as error context callback during startup procedure processing)

## Notes and Other Information
- This is a specialized error context callback used only during startup procedure processing
- Provides internationalization support through translator comments for the error message format
- The function is static, indicating it's only used within the pltcl.c module
- Follows PostgreSQL's error context callback pattern with void pointer argument
- Helps distinguish between trusted and untrusted PL/Tcl startup procedure errors
- Simple implementation that only adds context - does not handle or suppress errors