# SetupGOutput

## Location
src/bin/psql/common.c: 90 - 109

## Overview
Sets up query output redirection for the \g command in psql by opening the specified output file or pipe if needed.

## Definition
```c
static bool SetupGOutput(FILE **gfile_fout, bool *is_pipe)
```

## Detailed Description
This static function manages the setup of output redirection for psql's \g command. It checks if a global output file (pset.gfname) has been specified and if the corresponding file handle is not already open. When these conditions are met, it opens the output destination using openQueryOutputFile() and handles pipe-specific setup by disabling SIGPIPE traps to prevent premature termination when writing to pipes.

The function is designed to be called before query execution to ensure output redirection is properly configured. It maintains state consistency by only opening files when necessary and properly configuring pipe handling.

## Parameters / Member Variables
- `gfile_fout`: Pointer to FILE* that will hold the opened output file handle
- `is_pipe`: Pointer to bool flag that indicates whether the output is a pipe (affects signal handling)

## Dependencies
- Functions called/Symbols referenced:
  - [openQueryOutputFile](../o/openQueryOutputFile.md) (to open the output destination)
  - [disable_sigpipe_trap](../d/disable_sigpipe_trap.md) (to configure signal handling for pipes)
  - pset.gfname (global variable containing the output filename/command)
- Called from (representative examples):
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (multiple call sites)

## Notes and Other Information
- This is a static function, only accessible within common.c
- The function only opens a new output stream if pset.gfname is set and no stream is currently open
- SIGPIPE handling is automatically disabled for pipe outputs to prevent unexpected termination
- Returns false on error, true on success or when no action is needed
- Works in conjunction with CloseGOutput for proper resource management