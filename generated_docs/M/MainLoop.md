# MainLoop

## Location
src/bin/psql/mainloop.c: 33 - 662

## Overview
MainLoop is the core interactive command processor for psql that handles reading, parsing, and executing SQL commands and psql meta-commands from input sources, supporting both interactive sessions and script file processing.

## Definition
```c
int MainLoop(FILE *source)
```

## Detailed Description
MainLoop implements the central read-eval-print loop (REPL) for the psql command-line interface. This function is re-entrant and can be called recursively when processing \\i commands that read input from files. The function manages a complex state machine that handles:

- **Input Processing**: Reads lines from interactive terminals or script files, with support for readline history and UTF-8 BOM detection
- **Lexical Analysis**: Uses psql_scan to tokenize input, distinguishing between SQL commands, backslash commands, and special characters
- **Command Execution**: Executes SQL queries via SendQuery() and processes backslash commands via HandleSlashCmds()
- **Interactive Features**: Provides command-line editing, history management, help system, and user-friendly error handling
- **Conditional Processing**: Supports \\if/\\endif conditional blocks for script control flow
- **Error Handling**: Implements comprehensive error recovery with signal handling and graceful degradation

The function maintains several buffers to manage multi-line queries, command history, and supports features like query editing, prompt customization, and various echo modes. It handles both interactive user sessions and batch script processing seamlessly.

## Parameters / Member Variables
- `source`: FILE pointer to the input source (stdin for interactive mode, or file pointer for script processing)

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_create: Initialize lexical scanner
  - [conditional_stack_create](../c/conditional_stack_create.md): Create conditional processing stack for \\if blocks
  - createPQExpBuffer: Create expandable string buffers for queries
  - [gets_interactive](../g/gets_interactive.md): Read input line in interactive mode with readline support
  - [gets_fromFile](../g/gets_fromFile.md): Read input line from file
  - psql_scan: Tokenize and parse input line
  - [SendQuery](../S/SendQuery.md): Execute SQL query against database
  - [HandleSlashCmds](../H/HandleSlashCmds.md): Process psql backslash commands
  - [pg_append_history](../p/pg_append_history.md)/pg_send_history: Manage command history
  - get_prompt: Generate appropriate prompt based on parser state
  - [conditional_active](../c/conditional_active.md): Check if current \\if branch is active
- Called from (representative examples):
  - [process_file](../p/process_file.md): When processing \\i include files in command.c:4432
  - [main](../m/main.md): Primary entry point for psql interactive sessions

## Notes and Other Information
- **Re-entrancy**: The function is explicitly designed to be re-entrant to support nested file inclusion via \\i commands
- **Signal Handling**: Uses sigsetjmp/siglongjmp for graceful handling of SIGINT (Ctrl-C) interruptions
- **Memory Management**: Carefully manages multiple PQExpBuffer instances and ensures proper cleanup on exit
- **Compatibility Features**: Recognizes "help", "quit", and "exit" commands for compatibility with other SQL clients
- **Custom Dump Detection**: Automatically detects PostgreSQL custom-format dumps and provides helpful error messages
- **Multi-line Support**: Handles complex multi-line queries with proper history integration and prompt management
- **Error Recovery**: Implements sophisticated error recovery mechanisms including EOF handling with IGNOREEOF support
- **State Preservation**: Saves and restores previous command source state to support nested invocations
- **Exit Codes**: Returns appropriate exit codes (EXIT_SUCCESS, EXIT_FAILURE, EXIT_USER, EXIT_BADCONN) for different termination scenarios