# setQFout

## Location
src/bin/psql/common.c: 132 - 175

## Overview
Sets the query output destination for psql, handling both the -o command line option and the \o command for redirecting query results to files or pipes.

## Definition
```c
bool setQFout(const char *fname)
```

## Detailed Description
This function manages the redirection of query output in psql by safely switching from the current output destination to a new one. It performs the following key operations:

1. **Validation**: First attempts to open the new output destination to ensure it's valid before making any changes
2. **Safe transition**: Only closes the old output after successfully opening the new one
3. **Resource cleanup**: Properly closes the previous output file/pipe, capturing exit status for pipes
4. **State update**: Updates the global pset structure with the new output file and pipe status  
5. **Signal management**: Configures SIGPIPE handling appropriately for the new output type

The function ensures atomic behavior - either the output redirection succeeds completely, or the previous state is maintained unchanged.

## Parameters / Member Variables
- `fname`: Output destination specification - NULL/empty for stdout, '|command' for pipe, or filename for regular file

## Dependencies
- Functions called/Symbols referenced:
  - [openQueryOutputFile](../o/openQueryOutputFile.md) (to open and validate the new output destination)
  - [SetShellResultVariables](../S/SetShellResultVariables.md) (to capture pipe command exit status)
  - [pclose](../p/pclose.md) (to close pipe outputs)
  - fclose (to close regular file outputs)
  - [set_sigpipe_trap_state](set_sigpipe_trap_state.md) (to configure signal handling for pipes)
  - [restore_sigpipe_trap](../r/restore_sigpipe_trap.md) (to apply signal handling changes)
- Called from (representative examples):
  - [exec_command_out](../e/exec_command_out.md) (for \o command processing)
  - [parse_psql_options](../p/parse_psql_options.md) (for -o command line option)

## Notes and Other Information
- Returns false on error without changing pset state, true on success
- Protects stdout and stderr from being closed during output switching
- Automatically manages SIGPIPE signal handling based on output type (pipes vs files)
- The function provides transactional behavior - partial updates are not possible
- Used by both interactive commands (\o) and command-line options (-o)
- Exit status of pipe commands is captured and made available through shell result variables