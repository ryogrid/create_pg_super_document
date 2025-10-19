# pg_send_history

## Location
[src/bin/psql/input.c:135-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/input.c#L135-L185)

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
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - strlen
  - strcmp
  - free
  - [pg_strdup](pg_strdup.md)
  - add_history (readline function)
- Called from (representative examples):
  - [MainLoop](../M/MainLoop.md) (multiple locations)

## Notes and Other Information
- Only functions when USE_READLINE is defined and useHistory is enabled
- Uses a static variable prev_hist to remember the last history entry for duplicate detection
- Safely handles empty buffers - no action is taken if history_buf is empty
- Trims trailing newlines before processing to ensure clean history entries
- Increments history_lines_added counter to track the number of entries added
- The function can be called multiple times safely due to empty buffer checking
- Memory management: frees previous history string before allocating new one to prevent leaks

## Simplified Source

```c
void pg_send_history(PQExpBuffer history_buf) {
#ifdef USE_READLINE
    static char *prev_hist = NULL;
    char *s = history_buf->data;
    int i;

    // Trim trailing newlines from the command
    for (i = strlen(s) - 1; i >= 0 && s[i] == '\n'; i--) {
        // Continue trimming
    }
    s[i + 1] = '\0';

    // Add to history if enabled and not empty
    if (useHistory && s[0]) {
        // Check history control settings
        bool ignore_space = (pset.histcontrol & hctl_ignorespace) && s[0] == ' ';
        bool ignore_dup = (pset.histcontrol & hctl_ignoredups) &&
                         prev_hist && strcmp(s, prev_hist) == 0;

        if (ignore_space || ignore_dup) {
            // Skip this line according to history control rules
        } else {
            // Update previous history for duplicate checking
            free(prev_hist);
            prev_hist = pg_strdup(s);

            // Add to readline history and increment counter
            add_history(s);
            history_lines_added++;
        }
    }

    // Reset buffer for next command
    resetPQExpBuffer(history_buf);
#endif
}
```