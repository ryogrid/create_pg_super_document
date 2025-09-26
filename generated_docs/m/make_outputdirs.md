# make_outputdirs

## Location
[src/bin/pg_upgrade/pg_upgrade.c:249-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L249-L333)

## Overview
Creates and assigns proper permissions to the set of output directories used to store data generated internally by pg_upgrade, filling in log_opts structure with directory paths.

## Definition

```c
struct timeval time;
```
## Detailed Description
The make_outputdirs function is responsible for creating a structured directory hierarchy for pg_upgrade output files and logs. It creates a timestamped directory structure under the PostgreSQL data directory to organize upgrade-related files. The function:

- Creates a base output directory with timestamp-based subdirectories
- Establishes separate directories for dumps and logs  
- Sets up file handles for internal logging
- Initializes all log files with upgrade run timestamps
- Uses millisecond precision timestamps to avoid conflicts between concurrent runs

The directory structure created follows the pattern:  with subdirectories for dumps and logs.

## Parameters / Member Variables
- : Path to the PostgreSQL data directory where output directories will be created

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](../p/pg_malloc0.md) (memory allocation)
  - [gettimeofday](../g/gettimeofday.md) (high precision timestamp)
  - strftime (timestamp formatting)
  - mkdir (directory creation)
  - fopen_priv (secure file opening)
  - BASE_OUTPUTDIR (output directory name constant)
  - DUMP_OUTPUTDIR (dump subdirectory name)
  - LOG_OUTPUTDIR (log subdirectory name)
  - INTERNAL_LOG_FILE (internal log filename)
- Called from:
  - [main](main.md) (from pg_upgrade.c:124)

## Notes and Other Information
- Uses millisecond precision timestamps to prevent directory name collisions
- Handles the case where root directory already exists (for multiple upgrade attempts)
- Creates directories with pg_dir_create_mode permissions
- Initializes all output log files with upgrade start timestamps
- Critical for organizing pg_upgrade output and maintaining upgrade history
- Part of the pg_upgrade utility's initialization sequence