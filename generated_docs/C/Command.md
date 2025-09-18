# Command

## Location
src/bin/pgbench/pgbench.c: 740 - 754

## Overview
The Command structure represents a single command within a pgbench script, encapsulating both SQL and meta-commands along with their execution parameters, arguments, and performance statistics.

## Definition
```c
typedef struct Command
{
    PQExpBufferData lines;
    char       *first_line;
    int         type;
    MetaCommand meta;
    int         argc;
    char       *argv[MAX_ARGS];
    char       *prepname;
    char       *varprefix;
    PgBenchExpr *expr;
    SimpleStats stats;
    int64       retries;
    int64       failures;
} Command;
```

## Detailed Description
This structure serves as the fundamental building block for pgbench script execution, representing both SQL commands and meta-commands (pgbench-specific directives). Each Command contains the raw command text, parsed arguments, execution metadata, and performance tracking information. The structure supports advanced features like prepared statements, variable substitution, conditional execution, and comprehensive error tracking with retry logic.

## Parameters / Member Variables
- `lines`: Complete multi-line command text in raw form, without variable substitution applied
- `first_line`: Short single-line extract from 'lines' used for error reporting and logging purposes
- `type`: Command classification (SQL_COMMAND or META_COMMAND) determining how the command is processed
- `meta`: Specific meta-command type identifier, set to META_NONE/GSET/ASET for SQL commands
- `argc`: Number of parsed arguments in the command, zero if parsing has not yet occurred
- `argv`: Array of command arguments where argv[0] contains the processed command/SQL string with variable substitution
- `prepname`: Name under which this command is prepared when using prepared statement mode
- `varprefix`: Variable prefix for commands terminated with \gset or \aset, used to namespace result variables
- `expr`: Parsed expression tree for commands containing pgbench expressions requiring evaluation
- `stats`: Performance statistics tracking time spent executing this specific command
- `retries`: Count of retry attempts due to serialization or deadlock errors for this command
- `failures`: Number of non-retried errors that occurred while executing this command

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md)
  - [MetaCommand](../M/MetaCommand.md)
  - MAX_ARGS
  - [PgBenchExpr](../P/PgBenchExpr.md)
  - SimpleStats
- Called from (representative examples):
  - [ParsedScript](../P/ParsedScript.md)
  - [getQueryParams](../g/getQueryParams.md)
  - [prepareCommand](../p/prepareCommand.md)
  - [prepareCommandsInPipeline](../p/prepareCommandsInPipeline.md)
  - [sendCommand](../s/sendCommand.md)
  - [advanceConnectionState](../a/advanceConnectionState.md)
  - [executeMetaCommand](../e/executeMetaCommand.md)
  - [parseQuery](../p/parseQuery.md)
  - [create_sql_command](../c/create_sql_command.md)
  - [postprocess_sql_command](../p/postprocess_sql_command.md)
  - [process_backslash_command](../p/process_backslash_command.md)

## Notes and Other Information
Commands support sophisticated features including prepared statement caching, variable substitution with pgbench expressions, conditional execution flow, and detailed performance tracking. The structure is designed to handle both simple SQL statements and complex meta-commands that control script execution flow, making it the core abstraction for pgbench's scripting capabilities.