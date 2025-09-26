# simple_prompt

## Location
[src/common/sprompt.c:38-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sprompt.c#L38-L52)

## Overview
A generalized function for reading usernames and passwords interactively from the user, with support for hiding input (echo off) when needed.

## Definition
```c
char *simple_prompt(const char *prompt, bool echo)
```

## Detailed Description
The `simple_prompt` function is a wrapper around `simple_prompt_extended` that provides a simplified interface for interactive user input. It is specifically designed for reading sensitive information like usernames and passwords from the terminal. The function reads from `/dev/tty` on Unix-like systems or `stdin/stderr` as fallback, ensuring that input is obtained directly from the user terminal rather than being redirected from pipes or files.

The function automatically handles platform-specific terminal control to show or hide user input based on the `echo` parameter, making it suitable for password entry where characters should not be displayed on screen.

## Parameters / Member Variables
- `prompt`: The text prompt to display to the user, or NULL if no prompt is needed (automatically localized using gettext)
- `echo`: Boolean flag controlling input visibility - set to false for password input to hide characters, true to show typed characters

## Dependencies
- Functions called/Symbols referenced:
  - simple_prompt_extended
- Called from (representative examples):
  - get_su_pwd (src/bin/initdb/initdb.c:1652-1653)
  - GetConnection (src/bin/pg_basebackup/streamutil.c:165)
  - ConnectDatabase (src/bin/pg_dump/pg_backup_db.c:128, 178)
  - connectDatabase (src/bin/pg_dump/pg_dumpall.c:1771, 1873)
  - main (src/bin/psql/startup.c:243, 296)
  - yesno_prompt (src/bin/scripts/common.c:151)
  - main (src/bin/scripts/createuser.c:218, 233, 234)
  - main (src/bin/scripts/dropuser.c:118)
  - connectDatabase (src/fe_utils/connect_utils.c:49, 103)

## Notes and Other Information
- The returned string is malloc'd and the caller is responsible for freeing it when done
- Trailing newlines are automatically stripped from the input
- This is a simplified interface that internally calls simple_prompt_extended with NULL for the interrupt context
- Used extensively throughout PostgreSQL client tools for interactive authentication and user prompts
- The function handles cross-platform differences in terminal I/O automatically