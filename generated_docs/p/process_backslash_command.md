# process_backslash_command

## Location
src/bin/pgbench/pgbench.c: 5671 - 5880

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
  - initPQExpBuffer, termPQExpBuffer
  - expr_scanner_offset, expr_scanner_get_lineno, expr_scanner_get_substring
  - expr_lex_one_word
  - pg_malloc0, pg_strdup
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