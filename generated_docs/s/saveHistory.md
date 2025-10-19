# saveHistory

## Location
[src/bin/psql/input.c:413-493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/input.c#L413-L493)

## Overview
Saves the current readline history to a specified file with optional line limit truncation, handling newline encoding and concurrent session safety.

## Definition

```c
static bool
saveHistory(char *fname, int max_lines)
```
## Detailed Description
The  function persists the current readline command history to a file, implementing several sophisticated features for reliability and compatibility. Before saving, it encodes embedded newlines in multi-line commands to prevent readline from misinterpreting them as separate history entries when the file is later read.

The function implements two different saving strategies depending on available readline library features:

1. **Modern approach** (when HAVE_HISTORY_TRUNCATE_FILE and HAVE_APPEND_HISTORY are available): Uses atomic append operations to minimize conflicts with other concurrent psql sessions. It truncates the existing history file to the appropriate size and then appends only the new entries added during the current session.

2. **Fallback approach** (older readline versions): Uses the simpler write_history() function, which overwrites the entire history file but cannot safely handle concurrent access.

The function includes special handling for /dev/null as a history file destination, avoiding unnecessary write attempts that could fail due to file permission issues on some platforms like macOS.

## Parameters / Member Variables
- `*fname`: The target filename where history should be saved (char pointer)
- `max_lines`: Maximum number of history lines to preserve. If negative, saves all history lines. If non-negative, limits the total history size.
## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison for /dev/null check)
  - DEVNULL (constant representing /dev/null path)
  - [encode_history](../e/encode_history.md) (encodes newlines before saving)
  - history_truncate_file (truncates existing history file - if available)
  - append_history (appends new history entries - if available)
  - open (creates file if it doesn't exist)
  - close (closes file descriptor)
  - stifle_history (limits in-memory history size - fallback mode)
  - write_history (writes entire history - fallback mode)
  - pg_log_error (error reporting)
  - PG_BINARY (file mode flag for binary writing)
  - Max/Min (macros for mathematical operations)
  - history_lines_added (global counter of lines added in current session)

- Called from (representative examples):
  - [finishInput](../f/finishInput.md) (cleanup function called on program exit)

## Notes and Other Information
- Returns true on successful save, false on failure
- The /dev/null optimization prevents unnecessary chmod operations that would fail on some platforms
- Newline encoding is essential because readline treats embedded \n characters as line separators when reloading history
- The modern append-based approach reduces race conditions when multiple psql sessions exit simultaneously, though some race conditions remain
- File permissions are set to 0600 (user read/write only) for security
- The fallback mode using write_history() overwrites the entire file, potentially losing history from concurrent sessions
- When max_lines is specified, the function calculates how many lines to preserve from existing history versus newly added lines
- Error messages are logged using PostgreSQL's standard logging mechanism

## Simplified Source

```c
static bool saveHistory(char *fname, int max_lines) {
    int errnum;

    // Skip /dev/null to avoid chmod failures on macOS
    if (strcmp(fname, DEVNULL) != 0) {
        // Encode newlines for safe storage
        encode_history();

#if defined(HAVE_HISTORY_TRUNCATE_FILE) && defined(HAVE_APPEND_HISTORY)
        // Modern approach: truncate and append
        int nlines, fd;

        // Truncate existing history if needed
        if (max_lines >= 0) {
            nlines = Max(max_lines - history_lines_added, 0);
            history_truncate_file(fname, nlines);
        }

        // Ensure file exists for append_history
        fd = open(fname, O_CREAT | O_WRONLY | PG_BINARY, 0600);
        if (fd >= 0) close(fd);

        // Append new history entries
        nlines = (max_lines >= 0) ? Min(max_lines, history_lines_added) : history_lines_added;
        errnum = append_history(nlines, fname);
        if (errnum == 0) return true;
#else
        // Fallback: overwrite entire file
        if (max_lines >= 0) stifle_history(max_lines);
        errnum = write_history(fname);
        if (errnum == 0) return true;
#endif

        pg_log_error("could not save history to file \"%s\": %m", fname);
    }
    return false;
}
```