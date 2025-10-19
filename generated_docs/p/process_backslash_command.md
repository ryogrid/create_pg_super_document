# process_backslash_command

## Location
[src/bin/pgbench/pgbench.c:5671-5880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5671-L5880)

## Overview
Parses a backslash command in pgbench scripts and returns a Command struct representing the parsed metacommand.

## Definition
static Command *process_backslash_command(PsqlScanState sstate, const char *source)

## Detailed Description
This function handles the parsing of backslash commands (metacommands) in pgbench benchmark scripts. It scans the command line starting after the initial backslash, extracts the command name and arguments, validates syntax according to each command type, and creates a Command structure. The function supports various metacommands including set, sleep, shell operations, conditionals (if/elif/else/endif), and pipeline controls. For expression-based commands (set, if, elif), it invokes the expression parser to handle complex expressions.

## Parameters / Member Variables
- sstate: PsqlScanState scanner state for lexical analysis of the input
- source: Source string being parsed (for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md), termPQExpBuffer
  - expr_scanner_offset, expr_scanner_get_lineno, expr_scanner_get_substring
  - expr_lex_one_word
  - [pg_malloc0](pg_malloc0.md), pg_strdup
  - [initSimpleStats](../i/initSimpleStats.md)
  - [getMetaCommand](../g/getMetaCommand.md)
  - expr_scanner_init, expr_yyparse, expr_scanner_finish
  - [syntax_error](../s/syntax_error.md)
  - [pg_strcasecmp](pg_strcasecmp.md)
  - isdigit
  - [Command](../C/Command.md), PsqlScanState, PQExpBufferData structs
  - META_COMMAND, META_SET, META_IF, META_ELIF, META_SLEEP, META_SHELL, etc. enums
- Called from:
  - COMMANDS_ALLOC_NUM (src/bin/pgbench/pgbench.c:6000)

## Notes and Other Information
- Returns NULL if the line is a comment or empty
- Performs extensive syntax validation for each metacommand type
- Special handling for sleep command to parse time values with units (us/ms/s)
- Uses expression parser for set, if, and elif commands
- Maintains argument offsets for precise error reporting
- Part of pgbench's script parsing infrastructure
- Exits program on syntax errors rather than returning error codes

## Simplified Source

```c
static Command *process_backslash_command(PsqlScanState sstate, const char *source)
{
    Command *my_command;
    PQExpBufferData word_buf;
    int word_offset;
    int offsets[MAX_ARGS];
    int start_offset;
    int lineno;
    int j;

    initPQExpBuffer(&word_buf);

    // Remember backslash location for error reporting
    start_offset = expr_scanner_offset(sstate) - 1;
    lineno = expr_scanner_get_lineno(sstate, start_offset);

    // Get first word (command name)
    if (!expr_lex_one_word(sstate, &word_buf, &word_offset)) {
        termPQExpBuffer(&word_buf);
        return NULL;
    }

    // Initialize command structure
    my_command = (Command *) pg_malloc0(sizeof(Command));
    my_command->type = META_COMMAND;
    my_command->argc = 0;
    initSimpleStats(&my_command->stats);

    // Store command name
    j = 0;
    offsets[j] = word_offset;
    my_command->argv[j++] = pg_strdup(word_buf.data);
    my_command->argc++;

    // Convert to enum form
    my_command->meta = getMetaCommand(my_command->argv[0]);

    // Handle expression-based commands (set, if, elif)
    if (my_command->meta == META_SET || my_command->meta == META_IF || my_command->meta == META_ELIF) {
        yyscan_t yyscanner;

        // For \set, collect variable name
        if (my_command->meta == META_SET) {
            if (!expr_lex_one_word(sstate, &word_buf, &word_offset))
                syntax_error(source, lineno, my_command->first_line, my_command->argv[0],
                           "missing argument", NULL, -1);

            offsets[j] = word_offset;
            my_command->argv[j++] = pg_strdup(word_buf.data);
            my_command->argc++;
        }

        // Parse expression for all three command types
        yyscanner = expr_scanner_init(sstate, source, lineno, start_offset, my_command->argv[0]);

        if (expr_yyparse(yyscanner) != 0)
            exit(1);

        my_command->expr = expr_parse_result;
        my_command->first_line = expr_scanner_get_substring(sstate, start_offset,
                                                          expr_scanner_offset(sstate), true);
        expr_scanner_finish(yyscanner);
        termPQExpBuffer(&word_buf);
        return my_command;
    }

    // For other commands, collect remaining arguments
    while (expr_lex_one_word(sstate, &word_buf, &word_offset)) {
        if (j >= MAX_ARGS)
            syntax_error(source, lineno, my_command->first_line, my_command->argv[0],
                       "too many arguments", NULL, -1);

        offsets[j] = word_offset;
        my_command->argv[j++] = pg_strdup(word_buf.data);
        my_command->argc++;
    }

    // Save command line for error reporting
    my_command->first_line = expr_scanner_get_substring(sstate, start_offset,
                                                      expr_scanner_offset(sstate), true);

    // Validate command-specific syntax
    if (my_command->meta == META_SLEEP) {
        // Validate sleep command arguments and time units
        if (my_command->argc < 2)
            syntax_error(source, lineno, my_command->first_line, my_command->argv[0],
                       "missing argument", NULL, -1);
        // Additional sleep-specific validation...
    }
    else if (my_command->meta == META_SETSHELL) {
        if (my_command->argc < 3)
            syntax_error(source, lineno, my_command->first_line, my_command->argv[0],
                       "missing argument", NULL, -1);
    }
    else if (my_command->meta == META_SHELL) {
        if (my_command->argc < 2)
            syntax_error(source, lineno, my_command->first_line, my_command->argv[0],
                       "missing command", NULL, -1);
    }
    else if (my_command->meta == META_ELSE || my_command->meta == META_ENDIF ||
             my_command->meta == META_STARTPIPELINE || my_command->meta == META_ENDPIPELINE ||
             my_command->meta == META_SYNCPIPELINE) {
        if (my_command->argc != 1)
            syntax_error(source, lineno, my_command->first_line, my_command->argv[0],
                       "unexpected argument", NULL, -1);
    }
    else if (my_command->meta == META_GSET || my_command->meta == META_ASET) {
        if (my_command->argc > 2)
            syntax_error(source, lineno, my_command->first_line, my_command->argv[0],
                       "too many arguments", NULL, -1);
    }
    else {
        syntax_error(source, lineno, my_command->first_line, my_command->argv[0],
                   "invalid command", NULL, -1);
    }

    termPQExpBuffer(&word_buf);
    return my_command;
}
```