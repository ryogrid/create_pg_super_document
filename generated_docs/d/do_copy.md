# do_copy

## Location
[src/bin/psql/copy.c:268-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/copy.c#L268-L433)

## Overview
Executes a psql \copy command by opening the appropriate file/stream and coordinating with the PostgreSQL backend to perform the data transfer.

## Definition
```c
bool
do_copy(const char *args)
```

## Detailed Description
This is the main execution function for psql's \copy command (frontend copy). It parses the command arguments, opens the appropriate input or output stream (file, program, stdin, stdout, etc.), constructs the corresponding SQL COPY command, and coordinates the data transfer between the stream and the PostgreSQL backend. The function handles both COPY FROM (data input) and COPY TO (data output) operations, manages file operations including programs via popen, performs error checking on files and directories, and ensures proper cleanup of resources.

## Parameters / Member Variables
- `args`: String containing the complete \copy command arguments to be parsed and executed

## Dependencies
- Functions called/Symbols referenced:
  - [parse_slash_copy](../p/parse_slash_copy.md) (command parsing)
  - [canonicalize_path_enc](../c/canonicalize_path_enc.md) (path normalization)
  - popen, fopen, fclose, pclose (file operations)
  - fstat, S_ISDIR (file system checks)
  - [initPQExpBuffer](../i/initPQExpBuffer.md), printfPQExpBuffer, appendPQExpBufferStr, termPQExpBuffer (query building)
  - [SendQuery](../S/SendQuery.md) (query execution)
  - [free_copy_options](../f/free_copy_options.md) (cleanup)
  - [disable_sigpipe_trap](disable_sigpipe_trap.md), restore_sigpipe_trap (signal handling)
  - [SetShellResultVariables](../S/SetShellResultVariables.md) (result handling)
  - [wait_result_to_str](../w/wait_result_to_str.md) (error reporting)
- Called from (representative examples):
  - psql command processor (main psql loop)

## Notes and Other Information
- Returns true on success, false on failure
- Supports both regular files and program execution via popen
- Handles special stream destinations (stdin, stdout, pstdin, pstdout)
- Performs safety checks to prevent copying from/to directories
- Manages proper signal handling for program execution
- Uses pset.copyStream to coordinate with the SendQuery infrastructure
- Constructs SQL COPY commands with STDIN/STDOUT redirection for frontend processing
- Essential component of psql's data import/export functionality
- Properly handles cleanup of resources in all code paths including error conditions