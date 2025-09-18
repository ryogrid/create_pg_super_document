# gets_interactive

## Location
src/bin/psql/input.c: 67 - 112

## Overview
Gets a line of interactive input from the user, utilizing readline functionality when available for enhanced editing capabilities.

## Definition


## Detailed Description
This function is the primary interface for reading interactive input in psql. It provides a unified way to get user input while supporting both readline-enhanced input (when USE_READLINE is defined and useReadline is true) and basic input from stdin. The function handles signal management for SIGINT interrupts and integrates with tab completion functionality when readline is available.

When readline is available, the function:
- Resets the screen size to handle SIGWINCH signals
- Sets up the query buffer for tab completion callbacks
- Enables SIGINT handling via longjmp mechanism
- Uses readline() for enhanced input editing
- Properly cleans up after input is received

When readline is not available, it falls back to a simple prompt display followed by reading from stdin using gets_fromFile().

## Parameters / Member Variables
- `prompt`: The prompt string to display to the user
- `query_buf`: Buffer containing lines already read in the current command (used for tab completion context, not modified by this function)

## Dependencies
- Functions called/Symbols referenced:
  - USE_READLINE (preprocessor macro)
  - [gets_fromFile](gets_fromFile.md)
  - readline (when USE_READLINE is enabled)
  - rl_reset_screen_size (when HAVE_RL_RESET_SCREEN_SIZE is defined)
- Called from (representative examples):
  - [MainLoop](../M/MainLoop.md)

## Notes and Other Information
- The caller must have set up sigint_interrupt_jmp before calling this function
- Returns a malloc'd string that must be freed by the caller
- Thread safety depends on the underlying readline implementation
- The function handles SIGWINCH signals by resetting screen size when using readline
- Tab completion functionality is available through the query_buf parameter when readline is enabled