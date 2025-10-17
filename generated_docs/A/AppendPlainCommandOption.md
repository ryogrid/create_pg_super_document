# AppendPlainCommandOption

## Location
[src/bin/pg_basebackup/streamutil.c:812-832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L812-L832)

## Overview
Appends a "plain" option (option without a value) to a PostgreSQL replication command string, handling both old and new syntax formats with appropriate separators.

## Definition

```c
void
AppendPlainCommandOption(PQExpBuffer buf, bool use_new_option_syntax,
						 char *option_name)
```
## Detailed Description
This utility function builds PostgreSQL replication protocol commands by appending option names without values to a command buffer. It handles the syntax differences between old parser keyword style (space-separated) and new parenthesized comma-separated list style. The function intelligently adds appropriate separators (comma and space for new syntax, just space for old syntax) based on the current buffer state and syntax mode being used.

## Parameters / Member Variables
- `buf`: PQExpBuffer containing the command being constructed
- `use_new_option_syntax`: Boolean flag indicating which syntax format to use (true for new comma-separated syntax, false for old space-separated syntax)
- `option_name`: Name of the option to append (without a value)

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md) - [Append](Append.md) string to buffer
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md) - [Append](Append.md) single character to buffer
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) - [Append](Append.md) formatted string to buffer
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) (pg_basebackup.c:1895, 1903, 1905, 1912, 1919, 1925, 1932, 1955)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (streamutil.c:685, 691, 701)
  - [AppendStringCommandOption](AppendStringCommandOption.md) (streamutil.c:836)
  - [AppendIntegerCommandOption](AppendIntegerCommandOption.md) (streamutil.c:859)

## Notes and Other Information
- Handles two PostgreSQL command syntax formats:
  - Old syntax: COMMAND OPTION1 OPTION2 'value' OPTION3 42
  - New syntax: COMMAND (OPTION1, OPTION2 'value', OPTION3 42)
- Automatically detects if separator is needed by checking buffer state (avoids separator after opening parenthesis)
- Used for building replication protocol commands like BASE_BACKUP and CREATE_REPLICATION_SLOT
- Part of a family of command option appending functions (along with AppendStringCommandOption and AppendIntegerCommandOption)
- Always adds a space before the option name regardless of syntax mode
- In new syntax mode, adds ", " separator before the space and option name when needed
- In old syntax mode, adds just " " separator when needed

## Simplified Source

```c
void AppendPlainCommandOption(PQExpBuffer buf, bool use_new_option_syntax,
                             char *option_name) {
    // Add separator if buffer has content and doesn't end with '('
    if (buf->len > 0 && buf->data[buf->len - 1] != '(') {
        if (use_new_option_syntax)
            appendPQExpBufferStr(buf, ", ");  // New syntax: comma separator
        else
            appendPQExpBufferChar(buf, ' ');  // Old syntax: space separator
    }

    // Append the option name with a leading space
    appendPQExpBuffer(buf, " %s", option_name);
}
```