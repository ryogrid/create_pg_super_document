# saveHistory

## Location
src/bin/psql/input.c: 413 - 493

## Overview
Saves the current readline history to a specified file with optional line limit truncation, handling newline encoding and concurrent session safety.

## Definition


## Detailed Description
The  function persists the current readline command history to a file, implementing several sophisticated features for reliability and compatibility. Before saving, it encodes embedded newlines in multi-line commands to prevent readline from misinterpreting them as separate history entries when the file is later read.

The function implements two different saving strategies depending on available readline library features:

1. **Modern approach** (when HAVE_HISTORY_TRUNCATE_FILE and HAVE_APPEND_HISTORY are available): Uses atomic append operations to minimize conflicts with other concurrent psql sessions. It truncates the existing history file to the appropriate size and then appends only the new entries added during the current session.

2. **Fallback approach** (older readline versions): Uses the simpler write_history() function, which overwrites the entire history file but cannot safely handle concurrent access.

The function includes special handling for /dev/null as a history file destination, avoiding unnecessary write attempts that could fail due to file permission issues on some platforms like macOS.

## Parameters / Member Variables
- : The target filename where history should be saved (char pointer)
- : Maximum number of history lines to preserve. If negative, saves all history lines. If non-negative, limits the total history size.

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison for /dev/null check)
  - DEVNULL (constant representing /dev/null path)
  - encode_history (encodes newlines before saving)
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
  - finishInput (cleanup function called on program exit)

## Notes and Other Information
- Returns true on successful save, false on failure
- The /dev/null optimization prevents unnecessary chmod operations that would fail on some platforms
- Newline encoding is essential because readline treats embedded \n characters as line separators when reloading history
- The modern append-based approach reduces race conditions when multiple psql sessions exit simultaneously, though some race conditions remain
- File permissions are set to 0600 (user read/write only) for security
- The fallback mode using write_history() overwrites the entire file, potentially losing history from concurrent sessions
- When max_lines is specified, the function calculates how many lines to preserve from existing history versus newly added lines
- Error messages are logged using PostgreSQL's standard logging mechanism