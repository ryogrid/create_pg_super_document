# printHistory

## Location
[src/bin/psql/input.c:494-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/input.c#L494-L539)

## Overview
Displays the readline command history either to the console (with optional pager support) or saves it to a specified file without encoding transformations.

## Definition

```c
bool
printHistory(const char *fname, unsigned short int pager)
```
## Detailed Description
The  function provides a way to view or export the current readline command history in a human-readable format. Unlike , this function does not perform any newline encoding transformations, making it suitable for direct viewing and the psql \s command implementation.

The function supports two distinct output modes:

1. **Console output** (when fname is NULL): Displays history using the pager system if enabled, allowing users to scroll through long command histories comfortably. The pager behavior follows psql's global pager settings.

2. **File output** (when fname is provided): Writes the history directly to the specified file in plain text format, suitable for external processing or backup purposes.

The function was specifically designed to replace the previous use of  for display purposes because  doesn't support pager output and has compatibility issues with libedit implementations that prefer to encode their output format.

## Parameters / Member Variables
- `*fname`: Target filename for history output. If NULL, output goes to console with optional pager support.
- `pager`: Flag controlling pager usage when outputting to console (non-zero enables pager).
## Dependencies
- Functions called/Symbols referenced:
  - useHistory (global variable indicating if history is enabled)
  - [PageOutput](../P/PageOutput.md) (initializes pager output stream for console display)
  - fopen (opens file for writing when saving to file)
  - BEGIN_ITERATE_HISTORY (macro for starting history iteration)
  - END_ITERATE_HISTORY (macro for ending history iteration)
  - fprintf (writes history lines to output stream)
  - [ClosePager](../C/ClosePager.md) (finalizes pager output)
  - fclose (closes file stream)
  - pg_log_error (error reporting)
  - pset.popt.topt (global pager options structure)

- Called from (representative examples):
  - [exec_command_s](../e/exec_command_s.md) (implements psql \s command in src/bin/psql/command.c:2404)

## Notes and Other Information
- Returns true on successful output, false on failure or when history is not available
- Only available when compiled with USE_READLINE support
- Does not perform newline encoding unlike saveHistory(), preserving original command formatting
- When outputting to console without a filename, automatically uses pager if enabled in psql settings
- File output mode creates files with standard write permissions
- The function iterates through history entries in their stored order
- Error handling includes specific messages for file creation failures
- Designed specifically to avoid the encoding and compatibility issues present in saveHistory() for display purposes
- The pager integration allows comfortable viewing of long command histories without overwhelming the terminal