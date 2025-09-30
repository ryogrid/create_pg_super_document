# ClosePipeFromProgram

## Location
[src/backend/commands/copyfrom.c:1813-1842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfrom.c#L1813-L1842)

## Overview
Closes the pipe from an external program and performs proper error checking on the pclose() return code, handling expected and unexpected failure scenarios during COPY FROM PROGRAM operations.

## Definition

```c
static void
ClosePipeFromProgram(CopyFromState cstate)
```
## Detailed Description
This function is responsible for properly closing a pipe connection to an external program that was opened as part of a COPY FROM PROGRAM operation. It wraps the ClosePipeStream() call and provides comprehensive error handling for various failure scenarios.

The function distinguishes between expected and unexpected program failures:
- Expected failures include SIGPIPE when the COPY operation ends before reaching EOF, which is normal behavior
- Unexpected failures trigger detailed error reports with program name and failure details

The error handling ensures that PostgreSQL can provide meaningful diagnostic information when external programs fail during COPY operations while avoiding false alarms for expected termination scenarios.

## Parameters / Member Variables
- : CopyFromState structure containing the state of the COPY FROM operation, including the pipe file handle and metadata about the operation status

## Dependencies
- Functions called/Symbols referenced:
  - [ClosePipeStream](ClosePipeStream.md)
  - [wait_result_is_signal](../w/wait_result_is_signal.md)
  - [wait_result_to_str](../w/wait_result_to_str.md)
  - [errdetail_internal](../e/errdetail_internal.md)
  - SIGPIPE
- Called from (representative examples):
  - [EndCopyFrom](../E/EndCopyFrom.md)

## Notes and Other Information
- This is a static function used internally within the COPY FROM implementation
- The function specifically handles the SIGPIPE signal case, which can occur when a COPY FROM PROGRAM operation terminates before the external program finishes writing all its output
- Error reporting includes both generic error codes (ERRCODE_EXTERNAL_ROUTINE_EXCEPTION) and detailed internal error descriptions
- The function asserts that the cstate->is_program flag is set, ensuring it's only called for program-based COPY operations
- Located in src/backend/commands/copyfrom.c at lines 1813-1842

## Simplified Source

```c
static void ClosePipeFromProgram(CopyFromState cstate) {
    int pclose_rc;

    Assert(cstate->is_program);

    // Close the pipe and get return code
    pclose_rc = ClosePipeStream(cstate->copy_file);

    // Handle system errors
    if (pclose_rc == -1) {
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not close pipe to external command: %m")));
    }

    // Handle program failures
    if (pclose_rc != 0) {
        // SIGPIPE is expected if we ended before EOF
        if (!cstate->raw_reached_eof &&
            wait_result_is_signal(pclose_rc, SIGPIPE)) {
            return;
        }

        // Report unexpected failures
        ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                       errmsg("program \"%s\" failed", cstate->filename),
                       errdetail_internal("%s", wait_result_to_str(pclose_rc))));
    }
}
```