# getMetaCommand

## Location
src/bin/pgbench/pgbench.c: 2880 - 2921

## Overview
Converts a command name string to its corresponding meta-command enum identifier used in pgbench script processing.

## Definition


## Detailed Description
The `getMetaCommand` function serves as a command name parser that maps string representations of pgbench meta-commands to their corresponding enum values. It performs case-insensitive string comparisons using `pg_strcasecmp` to identify which meta-command type the input string represents. The function supports all pgbench meta-commands including variable assignment (`set`, `setshell`), control flow (`if`, `elif`, `else`, `endif`), system interaction (`shell`, `sleep`), result handling (`gset`, `aset`), and pipeline operations (`startpipeline`, `syncpipeline`, `endpipeline`). If the input is NULL or doesn't match any known command, it returns `META_NONE`.

## Parameters / Member Variables
- `cmd`: Null-terminated string containing the command name to be parsed (case-insensitive)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
- Types used:
  - [MetaCommand](../M/MetaCommand.md)
- Enum values referenced:
  - META_NONE
  - META_SET
  - META_SETSHELL
  - META_SHELL
  - META_SLEEP
  - META_IF
  - META_ELIF
  - META_ELSE
  - META_ENDIF
  - META_GSET
  - META_ASET
  - META_STARTPIPELINE
  - META_SYNCPIPELINE
  - META_ENDPIPELINE
- Called from (representative examples):
  - [process_backslash_command](../p/process_backslash_command.md)

## Notes and Other Information
- The function is declared as static, indicating it's for internal use within the pgbench module
- Uses case-insensitive comparison to provide user-friendly command parsing
- Returns META_NONE as a default/fallback value for unrecognized commands or NULL input
- Comprehensive coverage of all pgbench meta-command types including pipeline operations introduced in newer PostgreSQL versions
- The function serves as a centralized command recognition point, making it easy to add new meta-commands in the future