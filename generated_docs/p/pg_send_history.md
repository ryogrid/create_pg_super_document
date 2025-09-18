# pg_send_history

## Location
src/bin/psql/input.c: 135 - 185

## Overview
Processes accumulated history entries and sends them to readline's history mechanism while applying history control filters, then resets the buffer.

## Definition
```c
void pg_send_history(PQExpBuffer history_buf)
```

## Detailed Description
This function serves as the bridge between psql's internal history buffer and readline's history system. It processes the accumulated command text in the history buffer, applies various history control filters (such as ignoring duplicate entries or lines starting with spaces), and then adds qualified entries to readline's history.

The function implements two key history control features:
1. **ignorespace**: Ignores lines that start with a space character
2. **ignoredups**: Ignores lines that are identical to the previous history entry

Before sending to readline, the function cleans up the input by trimming trailing newlines. It maintains a static variable to track the previous history entry for duplicate detection. After processing, the history buffer is reset to empty, ready for the next command.

## Parameters / Member Variables
- `history_buf`: PQExpBuffer containing the accumulated command lines to be processed and added to history

## Dependencies
- Functions called/Symbols referenced:
  - USE_READLINE (preprocessor macro)
  - hctl_ignorespace
  - hctl_ignoredups
  - resetPQExpBuffer
  - strlen
  - strcmp
  - free
  - pg_strdup
  - add_history (readline function)
- Called from (representative examples):
  - MainLoop (multiple locations)

## Notes and Other Information
- Only functions when USE_READLINE is defined and useHistory is enabled
- Uses a static variable prev_hist to remember the last history entry for duplicate detection
- Safely handles empty buffers - no action is taken if history_buf is empty
- Trims trailing newlines before processing to ensure clean history entries
- Increments history_lines_added counter to track the number of entries added
- The function can be called multiple times safely due to empty buffer checking
- Memory management: frees previous history string before allocating new one to prevent leaks