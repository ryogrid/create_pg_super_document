# InheritStdHandles

## Location
[src/bin/pg_ctl/pg_ctl.c:1732-1755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1732-L1755)

## Overview
Sets up standard handle inheritance for a new Windows process to ensure it runs in a similar environment as the parent process when started as a service.

## Definition


## Detailed Description
This function configures the STARTUPINFO structure for a new process to inherit standard handles (stdin, stdout, stderr) from the current process. It addresses a specific Windows service behavior where processes started as services have NULL handles rather than invalid ones. The function ensures that if a standard handle is NULL, it gets replaced with INVALID_HANDLE_VALUE, which makes GetStdHandle() in the new process return INVALID_HANDLE_VALUE consistently. This creates a uniform environment between pg_ctl and the postmaster process it starts.

## Parameters / Member Variables
- : Pointer to STARTUPINFO structure that will be configured with standard handle inheritance settings

## Dependencies
- Functions called/Symbols referenced:
  - GetStdHandle (Windows API)
  - STARTF_USESTDHANDLES (Windows constant)
  - STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE (Windows constants)
  - INVALID_HANDLE_VALUE (Windows constant)
- Called from (representative examples):
  - [CreateRestrictedProcess](../C/CreateRestrictedProcess.md)

## Notes and Other Information
- This function is Windows-specific and addresses service-specific handle inheritance behavior
- The function modifies the dwFlags field to enable standard handle usage
- NULL handles are converted to INVALID_HANDLE_VALUE to ensure consistent behavior across process boundaries
- This ensures that the postmaster process runs in an environment similar to pg_ctl when started as a service
- The function is marked as static, limiting its scope to the pg_ctl.c file