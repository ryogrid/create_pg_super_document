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

## Simplified Source

```c
static void
getRestoreCommand(const char *argv0)
{
    int rc;
    char postgres_exec_path[MAXPGPATH];
    PQExpBuffer postgres_cmd;

    // Skip if WAL restore is not enabled
    if (!restore_wal)
        return;

    // Find postgres executable in same directory
    rc = find_other_exec(argv0, "postgres", PG_BACKEND_VERSIONSTR, postgres_exec_path);
    if (rc < 0)
    {
        // Handle executable not found or version mismatch
        char full_path[MAXPGPATH];
        if (find_my_exec(argv0, full_path) < 0)
            strlcpy(full_path, progname, sizeof(full_path));

        if (rc == -1)
            pg_fatal("postgres executable not found in same directory");
        else
            pg_fatal("postgres executable version mismatch");
    }

    // Build command: postgres -D datadir [-c config_file=...] -C restore_command
    postgres_cmd = createPQExpBuffer();
    appendShellString(postgres_cmd, postgres_exec_path);
    appendPQExpBufferStr(postgres_cmd, " -D ");
    appendShellString(postgres_cmd, datadir_target);

    // Add custom config file if specified
    if (config_file != NULL)
    {
        appendPQExpBufferStr(postgres_cmd, " -c config_file=");
        appendShellString(postgres_cmd, config_file);
    }

    appendPQExpBufferStr(postgres_cmd, " -C restore_command");

    // Execute command and read restore_command value
    restore_command = pipe_read_line(postgres_cmd->data);
    if (restore_command == NULL)
        pg_fatal("could not read restore_command from target cluster");

    pg_strip_crlf(restore_command);

    // Validate that restore_command is set
    if (strcmp(restore_command, "") == 0)
        pg_fatal("restore_command is not set in target cluster");

    pg_log_debug("using restore_command = '%s'", restore_command);
    destroyPQExpBuffer(postgres_cmd);
}
```