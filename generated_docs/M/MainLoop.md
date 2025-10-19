# MainLoop

## Location
[src/bin/psql/mainloop.c:33-662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/mainloop.c#L33-L662)

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
  - [createPQExpBuffer](../c/createPQExpBuffer.md): Create expandable string buffers for queries
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

## Simplified Source

```c
int MainLoop(FILE *source) {
    // Initialize state and buffers
    PsqlScanState scan_state = psql_scan_create(&psqlscan_callbacks);
    ConditionalStack cond_stack = conditional_stack_create();
    PQExpBuffer query_buf = createPQExpBuffer();
    PQExpBuffer previous_buf = createPQExpBuffer();
    PQExpBuffer history_buf = createPQExpBuffer();

    // Set up input source
    pset.cur_cmd_source = source;
    pset.cur_cmd_interactive = ((source == stdin) && !pset.notty);
    pset.lineno = 0;

    int successResult = EXIT_SUCCESS;

    // Main processing loop
    while (successResult == EXIT_SUCCESS) {
        // Handle Control-C interrupts
        if (cancel_pressed) {
            if (!pset.cur_cmd_interactive) {
                successResult = EXIT_USER;
                break;
            }
            cancel_pressed = false;
        }

        // Set up interrupt handling
        if (sigsetjmp(sigint_interrupt_jmp, 1) != 0) {
            // Reset state after interrupt
            psql_scan_reset(scan_state);
            resetPQExpBuffer(query_buf);
            resetPQExpBuffer(history_buf);
            continue;
        }

        // Read next line of input
        char *line;
        if (pset.cur_cmd_interactive) {
            line = gets_interactive(get_prompt(prompt_status, cond_stack), query_buf);
        } else {
            line = gets_fromFile(source);
        }

        // Handle end of input
        if (line == NULL) {
            if (pset.cur_cmd_interactive) {
                // Handle IGNOREEOF
                count_eof++;
                if (count_eof < pset.ignoreeof) {
                    printf("Use \"\\q\" to leave %s.\n", pset.progname);
                    continue;
                }
            }
            break;
        }

        pset.lineno++;

        // Skip empty lines and handle special cases
        if (line[0] == '\0' && !psql_scan_in_quote(scan_state)) {
            free(line);
            continue;
        }

        // Detect custom dump format
        if (pset.lineno == 1 && strncmp(line, "PGDMP", 5) == 0) {
            puts("Input is a PostgreSQL custom-format dump. Use pg_restore instead.");
            successResult = EXIT_FAILURE;
            break;
        }

        // Handle special interactive commands (help, quit, exit)
        if (pset.cur_cmd_interactive) {
            if (strncasecmp(line, "help", 4) == 0 && query_buf->len == 0) {
                // Show help message
                puts("You are using psql, the command-line interface to PostgreSQL.");
                free(line);
                continue;
            }
            if ((strncasecmp(line, "exit", 4) == 0 || strncasecmp(line, "quit", 4) == 0)
                && query_buf->len == 0) {
                successResult = EXIT_SUCCESS;
                break;
            }
        }

        // Add line to query buffer
        if (query_buf->len > 0)
            appendPQExpBufferChar(query_buf, '\n');

        // Parse and process the line
        psql_scan_setup(scan_state, line, strlen(line), pset.encoding, standard_strings());

        bool success = true;
        while (success || !die_on_error) {
            PsqlScanResult scan_result = psql_scan(scan_state, query_buf, &prompt_status);

            if (scan_result == PSCAN_SEMICOLON ||
                (scan_result == PSCAN_EOL && pset.singleline)) {
                // Execute complete SQL query
                if (conditional_active(cond_stack)) {
                    success = SendQuery(query_buf->data);
                    // Swap buffers for undo support
                    PQExpBuffer swap = previous_buf;
                    previous_buf = query_buf;
                    query_buf = swap;
                    resetPQExpBuffer(query_buf);
                }
            } else if (scan_result == PSCAN_BACKSLASH) {
                // Handle backslash command
                backslashResult slashResult = HandleSlashCmds(scan_state, cond_stack,
                                                            query_buf, previous_buf);
                success = (slashResult != PSQL_CMD_ERROR);

                if (slashResult == PSQL_CMD_SEND) {
                    success = SendQuery(query_buf->data);
                } else if (slashResult == PSQL_CMD_TERMINATE) {
                    successResult = EXIT_SUCCESS;
                    goto cleanup;
                }
            }

            // Break on end of line
            if (scan_result == PSCAN_INCOMPLETE || scan_result == PSCAN_EOL)
                break;
        }

        // Save to history if interactive
        if (pset.cur_cmd_interactive) {
            pg_append_history(line, history_buf);
            if (query_buf->len == 0)
                pg_send_history(history_buf);
        }

        free(line);

        // Check for errors in non-interactive mode
        if (!pset.cur_cmd_interactive && !success && die_on_error) {
            successResult = EXIT_USER;
        }
    }

    // Execute any remaining query at EOF
    if (query_buf->len > 0 && !pset.cur_cmd_interactive &&
        successResult == EXIT_SUCCESS) {
        if (conditional_active(cond_stack)) {
            SendQuery(query_buf->data);
        }
    }

cleanup:
    // Cleanup resources
    destroyPQExpBuffer(query_buf);
    destroyPQExpBuffer(previous_buf);
    destroyPQExpBuffer(history_buf);
    psql_scan_destroy(scan_state);
    conditional_stack_destroy(cond_stack);

    return successResult;
}
```