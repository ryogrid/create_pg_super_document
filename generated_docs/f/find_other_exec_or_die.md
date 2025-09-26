# find_other_exec_or_die

## Location
[src/bin/pg_ctl/pg_ctl.c:867-893](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L867-L893)

## Overview
Locates a required PostgreSQL executable program in the installation directory, terminating the process with an error message if the program cannot be found or has an incompatible version.

## Definition
```c
static char *find_other_exec_or_die(const char *argv0, const char *target, const char *versionstr)
```

## Detailed Description
This function serves as a wrapper around `find_other_exec` with added error handling and process termination. It is designed to locate essential PostgreSQL executables (such as postgres, initdb, etc.) that must be available for pg_ctl to function properly.

The function attempts to locate the target executable in the same directory as the current program, ensuring version compatibility. If the search fails for any reason, the function provides detailed error messages and terminates the process immediately, preventing pg_ctl from continuing with missing dependencies.

The error handling differentiates between two failure scenarios:
1. Program not found in the expected directory (-1 return code)
2. Program found but version mismatch (other negative return codes)

## Parameters / Member Variables
- `argv0`: The path/name used to invoke the current program (pg_ctl), used as a reference point for locating other executables
- `target`: Name of the target executable to find (e.g., "postgres", "initdb")
- `versionstr`: Expected version string for compatibility checking

## Dependencies
- Functions called/Symbols referenced:
  - `[pg_malloc](../p/pg_malloc.md)` (PostgreSQL memory allocation wrapper)
  - `[find_other_exec](find_other_exec.md)` (core executable location function)
  - `[find_my_exec](find_my_exec.md)` (function to determine current executable path)
  - `[strlcpy](../s/strlcpy.md)` (safe string copy utility)
  - [write_stderr](../w/write_stderr.md) (error output function)
  - `MAXPGPATH` (maximum path length constant)

- Called from:
  - [do_init](../d/do_init.md) (when initializing a database cluster)
  - [do_start](../d/do_start.md) (when starting the PostgreSQL server)
  - [adjust_data_dir](../a/adjust_data_dir.md) (when adjusting data directory paths)

## Notes and Other Information
- This function implements a "fail-fast" approach - if required executables are not available, it's better to terminate immediately than attempt to continue
- Memory is allocated for the result path but never freed within this function (caller responsibility)
- The function uses `find_my_exec` as a fallback to determine the current executable's path when constructing error messages
- Error messages are internationalized using the `_()" macro for translation support
- The function ensures that PostgreSQL tools work as a cohesive suite by enforcing co-location and version compatibility
- Return value should be freed by the caller when no longer needed
- Process termination with exit(1) makes this function unsuitable for library use - it's specifically designed for command-line tools