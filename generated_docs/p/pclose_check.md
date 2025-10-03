# pclose_check

## Location
[src/common/exec.c:410-447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/exec.c#L410-L447)

## Overview
A wrapper around the standard  function that provides enhanced error reporting and logging for pipe operations.

## Definition

```c
int
pclose_check(FILE *stream)
```
## Detailed Description
This function extends the standard  system call by adding comprehensive error reporting and logging capabilities. It closes a pipe stream created by  and examines the exit status of the child process. When errors occur, it provides detailed error messages using PostgreSQL's logging system, including human-readable descriptions of process termination reasons. The function helps distinguish between  failures and child process failures, making debugging easier.

## Parameters / Member Variables
- `*stream`: File pointer to the pipe stream that should be closed (previously opened with )
## Dependencies
- Functions called/Symbols referenced:
  -  - Standard library function to close pipe and wait for child process
  -  - PostgreSQL logging function for error messages
  -  - Converts wait status to human-readable string
  -  - PostgreSQL memory deallocation function
- Called from (representative examples):
  -  (src/bin/initdb/initdb.c:327)
  -  (src/common/exec.c:400)

## Notes and Other Information
- Returns 0 on successful closure and child process completion
- Returns the actual exit status from  on failure
- Distinguishes between  system call failures (exitstatus == -1) and child process failures
- Uses  to provide human-readable explanations of process termination (signals, exit codes, etc.)
- Follows PostgreSQL error reporting conventions with appropriate error codes
- Essential for robust pipe operation error handling in PostgreSQL utilities
- Memory management includes proper cleanup of dynamically allocated error message strings

## Simplified Source

```c
// Simplified version of pclose_check
int pclose_check(FILE *stream) {
    // Close the pipe and get exit status
    int exitstatus = pclose(stream);

    // Success case
    if (exitstatus == 0) {
        return 0;
    }

    // Handle pclose() system call failure
    if (exitstatus == -1) {
        log_error(errcode(ERRCODE_SYSTEM_ERROR),
                  _("%s() failed: %m"), "pclose");
    } else {
        // Handle child process failure - get readable error description
        char *reason = wait_result_to_str(exitstatus);
        log_error(errcode(ERRCODE_SYSTEM_ERROR), "%s", reason);
        pfree(reason);
    }

    return exitstatus;
}
```

Key simplifications made:
- Added clear comments for each logical section
- Separated success and error handling paths
- Maintained comprehensive error reporting functionality
- Preserved memory management for error strings