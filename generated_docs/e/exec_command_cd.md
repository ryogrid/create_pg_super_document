# exec_command_cd

## Location
[src/bin/psql/command.c:607-670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L607-L670)

## Overview
Implements the psql  command for changing the current working directory of the psql process.

## Definition

```c
struct passwd *pw;
```
## Detailed Description
The  function handles the  command in psql, which changes the current working directory similar to the shell's  command. When no directory argument is provided, it attempts to change to the user's home directory. The function includes platform-specific logic for determining the home directory on Unix-like systems versus Windows.

On Unix systems, it first checks the HOME environment variable, and if that's not available, it uses the system's user database (getpwuid) to determine the home directory. On Windows, it defaults to the root directory "/". The function provides appropriate error messages when directory changes fail.

## Parameters / Member Variables
- : Scanner state for parsing the command line arguments
- : Boolean indicating whether this command should be executed or just parsed
- : The command name ("cd") used for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option: Parses the directory argument from the command line
  - getenv: Retrieves HOME environment variable (Unix)
  - geteuid: Gets the effective user ID (Unix)
  - getpwuid: Retrieves user information by UID (Unix)
  - chdir: System call to change directory
  - pg_log_error: PostgreSQL logging function for error messages
  - [ignore_slash_options](../i/ignore_slash_options.md): Skips parsing when not in active branch
- Called from (representative examples):
  - [exec_command](exec_command.md): Main command dispatcher in psql

## Notes and Other Information
- Platform-specific behavior: Unix systems try HOME environment variable then user database, Windows defaults to root
- Provides detailed error messages including system error descriptions when directory changes fail
- Memory management handled properly with free() for allocated option string
- Returns PSQL_CMD_SKIP_LINE on success, PSQL_CMD_ERROR on failure
- Part of the psql interactive command system located in src/bin/psql/command.c:607-670

## Simplified Source

```c
static backslashResult
exec_command_cd(PsqlScanState scan_state, bool active_branch, const char *cmd)
{
    bool success = true;

    if (active_branch) {
        // Get directory argument or use default
        char *opt = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);
        char *dir;

        if (opt) {
            dir = opt;
        } else {
            // Get home directory
#ifndef WIN32
            // Unix: try HOME env var, then user database
            dir = getenv("HOME");
            if (!dir || dir[0] == '\0') {
                struct passwd *pw = getpwuid(geteuid());
                if (pw) {
                    dir = pw->pw_dir;
                } else {
                    pg_log_error("could not get home directory");
                    success = false;
                }
            }
#else
            // Windows: default to root
            dir = "/";
#endif
        }

        // Attempt directory change
        if (success && chdir(dir) < 0) {
            pg_log_error("\\%s: could not change directory to \"%s\": %m", cmd, dir);
            success = false;
        }

        free(opt);
    } else {
        ignore_slash_options(scan_state);
    }

    return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
}
```