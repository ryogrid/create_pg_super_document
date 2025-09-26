# check_exec

## Location
[src/bin/pg_upgrade/exec.c:429-459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/exec.c#L429-L459)

## Overview
Validates the existence, executability, and optionally the version of a specific PostgreSQL executable file within a given directory.

## Definition

```c
static void
check_exec(const char *dir, const char *program, bool check_version)
```
## Detailed Description
This function performs thorough validation of individual PostgreSQL executables required for the pg_upgrade process. It constructs the full path to the executable, verifies its existence and executability using validate_exec(), and tests that it can be executed by running it with the '-V' (version) flag.

When version checking is enabled, the function compares the executable's reported version string against the expected version format to ensure compatibility with the current pg_upgrade utility. This is particularly important for target cluster validation to prevent version mismatches that could cause upgrade failures.

The function uses pipe_read_line() to capture the executable's version output and performs string comparison to validate the version format matches the expected PostgreSQL version pattern.

## Parameters / Member Variables
- `dir`: Directory path where the executable should be located
- `program`: Name of the executable program to check (e.g., "postgres", "pg_ctl", "initdb")
- `check_version`: Boolean flag indicating whether to verify the executable's version matches the expected pg_upgrade version

## Dependencies
- Functions called/Symbols referenced:
  - [validate_exec](../v/validate_exec.md)
  - [pipe_read_line](../p/pipe_read_line.md)
  - [pg_strip_crlf](../p/pg_strip_crlf.md)
  - [pg_free](../p/pg_free.md)
  - snprintf
  - strcmp
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [check_bin_dir](check_bin_dir.md)

## Notes and Other Information
- Exits the program with a fatal error if the executable is missing, not executable, or fails version validation
- Uses the '-V' flag to retrieve version information from executables
- Version string format validation ensures compatibility between pg_upgrade and target executables
- Memory management includes proper cleanup of dynamically allocated line buffer
- Critical component of the binary directory validation process in pg_upgrade