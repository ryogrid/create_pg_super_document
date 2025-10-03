# process_psqlrc

## Location
[src/bin/psql/startup.c:774-807](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L774-L807)

## Overview
A static function responsible for loading psql configuration files (.psqlrc) from system and user locations during psql startup initialization.

## Definition

```c
static void
process_psqlrc(char *argv0)
```
## Detailed Description
This function implements the psql configuration file loading logic by searching for and processing .psqlrc files in a specific order of precedence. It first processes the system-wide configuration file located in the PostgreSQL installation's etc directory, then processes either a user-specified PSQLRC environment variable file or the default user .psqlrc file in the home directory. The function handles path resolution, tilde expansion, and ensures proper error handling for executable path detection.

## Parameters / Member Variables
- `*argv0`: The program name (argv[0]) used to determine the executable's location for finding the system configuration directory
## Dependencies
- Functions called/Symbols referenced:
  - [find_my_exec](../f/find_my_exec.md) (executable path resolution)
  - [get_etc_path](../g/get_etc_path.md) (system configuration directory path)
  - process_psqlrc_file (actual file processing)
  - [expand_tilde](../e/expand_tilde.md) (tilde expansion in paths)
  - [get_home_path](../g/get_home_path.md) (user home directory resolution)
  - [pstrdup](pstrdup.md) (string duplication)
  - getenv (environment variable access)
- Called from (representative examples):
  - [adhoc_opts](../a/adhoc_opts.md)
  - PARAMS_ARRAY_SIZE (startup processing)

## Notes and Other Information
- Configuration files are processed in order: system-wide SYSPSQLRC, then user-specific PSQLRC or default ~/.psqlrc
- The PSQLRC environment variable takes precedence over the default user configuration file location
- Tilde expansion is performed on PSQLRC environment variable paths
- Error handling includes fatal termination if the executable path cannot be determined
- Memory allocated for environment variable processing is properly managed
- Uses PostgreSQL-specific path constants MAXPGPATH, SYSPSQLRC, and PSQLRC