# openQueryOutputFile

## Location
[src/bin/psql/common.c:56-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L56-L89)

## Overview
Opens a query output file for psql, supporting standard output, regular files, and pipe commands with appropriate file handle management.

## Definition

```c
bool
openQueryOutputFile(const char *fname, FILE **fout, bool *is_pipe)
```
## Detailed Description
This function provides a unified interface for opening different types of output destinations in psql. It handles three distinct cases:
1. **Standard output**: When fname is NULL or empty, directs output to stdout
2. **Pipe command**: When fname starts with '|', executes the remainder as a shell command via popen()
3. **Regular file**: Opens a regular file for writing via fopen()

The function abstracts the complexity of different output types and provides consistent error handling. It flushes all open streams before opening a pipe to ensure proper output ordering.

## Parameters / Member Variables
- `*fname`: Output destination specification - NULL/empty for stdout, '|command' for pipe, or filename for regular file
- `**fout`: Pointer to FILE* where the opened file handle will be stored
- `*is_pipe`: Pointer to bool flag indicating whether the output is a pipe (affects cleanup behavior)
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