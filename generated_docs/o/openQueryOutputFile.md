# openQueryOutputFile

## Location
src/bin/psql/common.c: 56 - 89

## Overview
Opens a query output file for psql, supporting standard output, regular files, and pipe commands with appropriate file handle management.

## Definition


## Detailed Description
This function provides a unified interface for opening different types of output destinations in psql. It handles three distinct cases:
1. **Standard output**: When fname is NULL or empty, directs output to stdout
2. **Pipe command**: When fname starts with '|', executes the remainder as a shell command via popen()
3. **Regular file**: Opens a regular file for writing via fopen()

The function abstracts the complexity of different output types and provides consistent error handling. It flushes all open streams before opening a pipe to ensure proper output ordering.

## Parameters / Member Variables
- : Output destination specification - NULL/empty for stdout, '|command' for pipe, or filename for regular file
- : Pointer to FILE* where the opened file handle will be stored
- : Pointer to bool flag indicating whether the output is a pipe (affects cleanup behavior)

## Dependencies
- Functions called/Symbols referenced:
  - popen (for pipe commands)
  - fopen (for regular files)
  - fflush (to ensure output ordering before pipes)
  - pg_log_error (for error reporting)
- Called from (representative examples):
  - [SetupGOutput](../S/SetupGOutput.md)
  - [setQFout](../s/setQFout.md)

## Notes and Other Information
- The caller is responsible for managing SIGPIPE behavior when dealing with pipe outputs
- Error messages use the %m format specifier to include system error descriptions
- The function flushes all streams before opening pipes to prevent output ordering issues
- Returns false on error with appropriate error logging, true on success