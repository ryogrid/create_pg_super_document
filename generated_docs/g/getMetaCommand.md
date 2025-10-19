# getMetaCommand

## Location
[src/bin/pgbench/pgbench.c:2880-2921](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2880-L2921)

## Overview
Converts a command name string to its corresponding meta-command enum identifier used in pgbench script processing.

## Definition

```c
static MetaCommand
getMetaCommand(const char *cmd)
```
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

## Simplified Source

```c
static MetaCommand getMetaCommand(const char *cmd) {
    // Handle null input
    if (cmd == NULL)
        return META_NONE;

    // Map command strings to enum values (case-insensitive)
    if (pg_strcasecmp(cmd, "set") == 0)           return META_SET;
    if (pg_strcasecmp(cmd, "setshell") == 0)      return META_SETSHELL;
    if (pg_strcasecmp(cmd, "shell") == 0)         return META_SHELL;
    if (pg_strcasecmp(cmd, "sleep") == 0)         return META_SLEEP;
    if (pg_strcasecmp(cmd, "if") == 0)            return META_IF;
    if (pg_strcasecmp(cmd, "elif") == 0)          return META_ELIF;
    if (pg_strcasecmp(cmd, "else") == 0)          return META_ELSE;
    if (pg_strcasecmp(cmd, "endif") == 0)         return META_ENDIF;
    if (pg_strcasecmp(cmd, "gset") == 0)          return META_GSET;
    if (pg_strcasecmp(cmd, "aset") == 0)          return META_ASET;
    if (pg_strcasecmp(cmd, "startpipeline") == 0) return META_STARTPIPELINE;
    if (pg_strcasecmp(cmd, "syncpipeline") == 0)  return META_SYNCPIPELINE;
    if (pg_strcasecmp(cmd, "endpipeline") == 0)   return META_ENDPIPELINE;

    // Unknown command
    return META_NONE;
}
```