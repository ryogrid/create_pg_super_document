# finishInput

## Location
[src/bin/psql/input.c:540-550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/input.c#L540-L550)

## Overview
Cleanup function that saves command history to persistent storage and frees allocated memory when psql terminates.

## Definition

```c
static void
finishInput(void)
```
## Detailed Description
The  function serves as a cleanup handler for psql's input subsystem, automatically invoked when the program exits. Its primary responsibility is to ensure that the current session's command history is properly persisted to disk and that associated memory resources are freed.

The function performs two key cleanup operations:
1. **History persistence**: Saves the current readline history to the designated history file using the  function, respecting the maximum history size limit configured in .
2. **Memory cleanup**: Frees the dynamically allocated memory used for storing the history file path and resets the global pointer to prevent dangling references.

This function is typically registered as an exit handler using  during the initialization process, ensuring that history is preserved even if the program terminates unexpectedly through normal exit paths.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - useHistory (global variable indicating if history functionality is enabled)
  - psql_history (global variable containing the path to the history file)
  - [saveHistory](../s/saveHistory.md) (saves current history to specified file with line limit)
  - free (deallocates dynamically allocated memory)
  - pset.histsize (global setting for maximum history size)

- Called from (representative examples):
  - Registered with atexit() in initializeInput (src/bin/psql/input.c:399)
  - Automatically invoked during program termination

## Notes and Other Information
- Only available when compiled with USE_READLINE support
- The function is designed to be safe to call multiple times (subsequent calls do nothing after psql_history is set to NULL)
- History saving respects the histsize setting, which can limit the total number of history entries preserved
- The function ignores the return value of saveHistory(), as there's limited error recovery possible during program termination
- Memory cleanup prevents potential memory leaks, though this is primarily for code hygiene since the program is terminating
- The function only attempts to save history if both useHistory is enabled and psql_history contains a valid path
- Registration as an exit handler ensures history preservation across different termination scenarios (normal exit, signals, etc.)