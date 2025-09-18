# ClosePipeToProgram

## Location
[src/backend/commands/copyto.c:289-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L289-L313)

## Overview
Closes a pipe connection to an external program during COPY TO operations and handles error reporting based on the program's exit status.

## Definition
```c
static void ClosePipeToProgram(CopyToState cstate)
```

## Detailed Description
ClosePipeToProgram is responsible for properly terminating a pipe connection to an external program used in COPY TO PROGRAM operations. It calls ClosePipeStream() to close the pipe and then examines the return code to determine if the external program executed successfully. The function provides detailed error reporting, distinguishing between pipe closure failures and program execution failures.

When the program fails, it provides both a user-friendly error message indicating which program failed and detailed internal information about the exit status using wait_result_to_str() to decode the program's termination status.

## Parameters / Member Variables
- `cstate`: CopyToState structure containing the current state of the COPY operation, including the pipe file handle and program information

## Dependencies
- Functions called/Symbols referenced:
  - ClosePipeStream (closes the pipe and returns exit status)
  - ereport (error reporting function)
  - [errcode_for_file_access](../e/errcode_for_file_access.md) (error code for file access errors)
  - [errcode](../e/errcode.md) (generic error code function)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - [errdetail_internal](../e/errdetail_internal.md) (internal error details)
  - wait_result_to_str (converts wait status to string representation)
- Called from (representative examples):
  - DR_copy
  - [CopySendEndOfRow](CopySendEndOfRow.md)
  - [EndCopy](../E/EndCopy.md)

## Notes and Other Information
- This function is only called when cstate->is_program is true, as verified by the Assert
- Provides two types of error handling: pipe closure errors (-1 return) and program execution errors (non-zero exit status)
- Uses wait_result_to_str() to provide detailed information about how the program terminated (exit code, signal, etc.)
- Part of PostgreSQL's COPY TO PROGRAM feature that allows piping data directly to external commands
- The function ensures proper cleanup and meaningful error reporting for failed external programs