# editFile

## Location
[src/bin/psql/command.c:4103-4184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L4103-L4184)

## Overview
A helper function that invokes an external text editor to edit a specified file, with optional line number positioning support.

## Definition
```c
static bool editFile(const char *fname, int lineno)
```

## Detailed Description
This function serves as the core editor invocation mechanism for psql's \e and \ef commands. It determines which editor to use by checking environment variables in order of preference, constructs an appropriate command line (handling platform-specific quoting requirements), and executes the editor via the system shell.

The function handles both simple file editing and editor positioning to a specific line number, making it suitable for editing SQL queries and functions where precise cursor positioning is desired. It includes robust error handling for various failure scenarios and platform-specific adaptations for Windows vs. Unix-like systems.

## Parameters / Member Variables
- `fname`: The absolute path to the file that should be edited
- `lineno`: The line number where the editor should position the cursor (if > 0); 0 means no specific positioning

## Dependencies
- Functions called/Symbols referenced:
  - getenv() (multiple calls to check environment variables)
  - DEFAULT_EDITOR (fallback editor constant)
  - DEFAULT_EDITOR_LINENUMBER_ARG (platform-specific line number argument)
  - [psprintf](../p/psprintf.md)() (for command construction)
  - system() (for editor execution)
  - pg_log_error() (for error reporting)
  - fflush() (to flush output streams)
  - free() (to clean up allocated command string)
- Called from:
  - [do_edit](../d/do_edit.md) (at src/bin/psql/command.c:4302)

## Notes and Other Information
- Checks environment variables in this order: PSQL_EDITOR, EDITOR, VISUAL, then falls back to DEFAULT_EDITOR
- For line number positioning, uses PSQL_EDITOR_LINENUMBER_ARG or DEFAULT_EDITOR_LINENUMBER_ARG
- Uses different command quoting strategies for Windows vs. Unix due to shell differences
- Returns true on successful editor invocation (exit status 0), false otherwise
- Static function, only accessible within the command.c source file
- Handles special error cases: failed to start editor (-1) and failed to start shell (127)
- The function flushes all output streams before invoking the editor to ensure clean display
- On Unix systems, the editor value should not be pre-quoted as it may include command switches