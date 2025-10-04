# shell_archive_file

## Location
[src/backend/archive/shell_archive.c:57-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/archive/shell_archive.c#L57-L138)

## Overview
This function executes the configured shell command to archive a WAL file, handling command execution, error reporting, and platform-specific signal handling.

## Definition
```c
static bool shell_archive_file(ArchiveModuleState *state, const char *file, const char *path)
```

## Detailed Description
The `shell_archive_file` function is the core archiving callback that performs the actual WAL file archiving using a user-configured shell command. It takes the archive command template from `XLogArchiveCommand`, substitutes placeholders with the actual file name and path, executes the command using the system() call, and handles various error conditions.

The function performs the following key operations:
1. Converts the path to native format if provided
2. Substitutes `%f` and `%p` placeholders in the archive command with the file name and path
3. Executes the command while tracking wait events for monitoring
4. Analyzes the command's exit status and reports appropriate errors
5. Handles platform-specific signal reporting (Windows vs Unix)
6. Returns success/failure status to the calling archiving infrastructure

The function distinguishes between different types of failures: signal termination (which may indicate the archiver should be restarted), normal exit codes, and unrecognized statuses.

## Parameters / Member Variables
- `state`: Pointer to ArchiveModuleState structure (currently unused by this implementation)
- `file`: The name of the WAL file to be archived (used for %f placeholder substitution)
- `path`: The full path to the WAL file to be archived (used for %p placeholder substitution, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md) (memory allocation for path copy)
  - [make_native_path](../m/make_native_path.md) (path format conversion)
  - [replace_percent_placeholders](../r/replace_percent_placeholders.md) (placeholder substitution)
  - ereport/errmsg/errdetail (error reporting)
  - fflush (ensure output flushing)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/pgstat_report_wait_end (wait event tracking)
  - system (command execution)
  - [wait_result_is_any_signal](../w/wait_result_is_any_signal.md) (signal detection)
  - WIFEXITED, WIFSIGNALED, WEXITSTATUS, WTERMSIG (wait status macros)
  - [pg_strsignal](../p/pg_strsignal.md) (signal name resolution on Unix)
  - [pfree](../p/pfree.md) (memory cleanup)
  - elog (debug logging)
- Called from (representative examples):
  - Referenced indirectly through shell_archive_callbacks structure

## Notes and Other Information
- This is a static function, only accessible within the shell_archive.c module
- The function is assigned to the `archive_file_cb` member of the shell_archive_callbacks structure
- [Command](../C/Command.md) placeholders: `%f` is replaced with the file name, `%p` with the full path
- The function uses FATAL error level for signal-related failures to ensure the archiver process is restarted
- Platform differences: Windows reports exception codes while Unix systems report signal numbers and names
- Wait events are reported to PostgreSQL's statistics system for monitoring archive command duration
- The `state` parameter is currently unused but maintained for interface consistency
- Returns true on successful archiving, false on failure

## Simplified Source

```c
static bool
shell_archive_file(ArchiveModuleState *state, const char *file, const char *path)
{
    char *xlogarchcmd;
    char *nativePath = NULL;
    int rc;

    // Convert path to native format if provided
    if (path) {
        nativePath = pstrdup(path);
        make_native_path(nativePath);
    }

    // Build command by substituting %f and %p placeholders
    xlogarchcmd = replace_percent_placeholders(XLogArchiveCommand,
                                              "archive_command", "fp",
                                              file, nativePath);

    ereport(DEBUG3, (errmsg_internal("executing archive command \"%s\"", xlogarchcmd)));

    // Execute the archive command and track wait time
    fflush(NULL);
    pgstat_report_wait_start(WAIT_EVENT_ARCHIVE_COMMAND);
    rc = system(xlogarchcmd);
    pgstat_report_wait_end();

    // Handle command failure with detailed error reporting
    if (rc != 0) {
        int lev = wait_result_is_any_signal(rc, true) ? FATAL : LOG;

        if (WIFEXITED(rc)) {
            ereport(lev, (errmsg("archive command failed with exit code %d", WEXITSTATUS(rc)),
                         errdetail("The failed archive command was: %s", xlogarchcmd)));
        } else if (WIFSIGNALED(rc)) {
            // Platform-specific signal reporting
#if defined(WIN32)
            ereport(lev, (errmsg("archive command was terminated by exception 0x%X", WTERMSIG(rc)),
                         errdetail("The failed archive command was: %s", xlogarchcmd)));
#else
            ereport(lev, (errmsg("archive command was terminated by signal %d: %s",
                                WTERMSIG(rc), pg_strsignal(WTERMSIG(rc))),
                         errdetail("The failed archive command was: %s", xlogarchcmd)));
#endif
        } else {
            ereport(lev, (errmsg("archive command exited with unrecognized status %d", rc),
                         errdetail("The failed archive command was: %s", xlogarchcmd)));
        }

        pfree(xlogarchcmd);
        return false;
    }

    pfree(xlogarchcmd);
    elog(DEBUG1, "archived write-ahead log file \"%s\"", file);
    return true;
}
```