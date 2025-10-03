# getRestoreCommand

## Location
[src/bin/pg_rewind/pg_rewind.c:1056-1128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/pg_rewind.c#L1056-L1128)

## Overview
Retrieves the value of the restore_command GUC parameter from the target PostgreSQL cluster using the postgres executable's -C option.

## Definition

```c
static void
getRestoreCommand(const char *argv0)
```
## Detailed Description
This function is part of pg_rewind's WAL restoration mechanism. It dynamically retrieves the restore_command configuration parameter from the target cluster by:

1. Finding the postgres executable in the same directory as the current program
2. Building a command line that uses 'postgres -C restore_command' to query the GUC value
3. Executing the command and reading the output
4. Validating that restore_command is properly set (non-empty)

The function only executes if restore_wal is enabled. The retrieved restore_command will later be used to restore WAL files needed for the rewind operation.

## Parameters / Member Variables
- `*argv0`: The program name/path used to locate the postgres executable in the same directory
## Dependencies
- Functions called/Symbols referenced:
  - [find_other_exec](../f/find_other_exec.md)
  - [find_my_exec](../f/find_my_exec.md)  
  - [strlcpy](../s/strlcpy.md)
  - [pg_fatal](../p/pg_fatal.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendShellString](../a/appendShellString.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [pipe_read_line](../p/pipe_read_line.md)
  - [pg_strip_crlf](../p/pg_strip_crlf.md)
  - strcmp
  - pg_log_debug
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from (representative examples):
  - [main](../m/main.md) (pg_rewind.c)

## Notes and Other Information
- This is a static function local to pg_rewind.c
- Only executes when restore_wal is true
- Uses PG_BACKEND_VERSIONSTR to verify postgres executable version compatibility
- Supports custom configuration files via the global config_file variable
- The retrieved restore_command is stored in the global restore_command variable
- Will terminate the program if postgres executable is not found or restore_command is empty
- Located at src/bin/pg_rewind/pg_rewind.c:1056-1128