# LogOpts

## Location
[src/bin/pg_upgrade/pg_upgrade.h:316-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.h#L316-L329)

## Overview
LogOpts is a configuration structure that manages logging and output options for the pg_upgrade utility, controlling verbosity, log file handling, and output directory organization.

## Definition

```c
typedef struct
{
	bool		check;			/* check clusters only, don't change any data */
	bool		do_sync;		/* flush changes to disk */
	transferMode transfer_mode; /* copy files or link them? */
	int			jobs;			/* number of processes/threads to use */
	char	   *socketdir;		/* directory to use for Unix sockets */
	char	   *sync_method;
} UserOpts;
```
## Detailed Description
LogOpts centralizes all logging and output configuration for the pg_upgrade process. It manages both the behavioral aspects of logging (verbosity levels, log retention) and the organizational structure of output files through a hierarchy of directories. The structure enables pg_upgrade to provide comprehensive logging while maintaining organized output for debugging and audit purposes.

## Parameters / Member Variables
- `internal`: FILE pointer for internal logging operations, used for detailed diagnostic output
- `verbose`: Boolean flag controlling verbosity level; when true, enables detailed progress and diagnostic messages
- `retain`: Boolean flag determining whether log files should be preserved after successful completion
- `rootdir`: Root directory path (typically "pg_upgrade_output.d") containing all upgrade-related output
- `basedir`: Base output directory path with timestamp, providing unique storage for each upgrade run
- `dumpdir`: Directory path specifically designated for database dump files during the upgrade process
- `logdir`: Directory path for storing various log files generated during upgrade operations
- `isatty`: Boolean indicator of whether stdout is connected to a terminal (tty), affecting output formatting

## Dependencies
- Functions called/Symbols referenced:
  - FILE
  - transferMode (indirectly through related structures)
- Called from (representative examples):
  - [OSInfo](../O/OSInfo.md) (as part of larger configuration structures)

## Notes and Other Information
- This structure is essential for pg_upgrade's comprehensive logging and output management strategy
- The directory hierarchy (rootdir/basedir/dumpdir, logdir) provides organized storage for different types of output files
- The isatty flag enables appropriate output formatting for both interactive and non-interactive usage
- Log retention behavior can be controlled to aid in troubleshooting failed upgrades while cleaning up successful ones
- Verbose mode provides detailed progress information crucial for monitoring long-running upgrade operations
- Internal logging provides diagnostic information separate from user-facing progress messages