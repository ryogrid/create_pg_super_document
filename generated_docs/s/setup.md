# setup

## Location
[src/bin/pg_upgrade/pg_upgrade.c:334-404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L334-L404)

## Overview
Performs essential initialization tasks for pg_upgrade including environment validation, binary directory setup, and postmaster process management to ensure a clean upgrade environment.

## Definition

```c
static void
setup(char *argv0, bool *live_check)
```
## Detailed Description
The setup function prepares the pg_upgrade environment by performing critical initialization and validation tasks. It:

- Validates the environment variables to prevent libpq connection issues
- Sets default binary directory if not specified by user (-B option)
- Verifies that required directories exist and are accessible
- Checks for running postmaster processes on both old and new clusters
- Handles stale postmaster.pid files by attempting server startup/shutdown
- Sets live_check flag when existing servers are detected during check mode

The function ensures that no postmaster processes interfere with the upgrade process, while allowing for "live check" mode where existing servers can remain running during validation.

## Parameters / Member Variables
- `*argv0`: Path to the pg_upgrade executable (used to determine default binary directory)
- `*live_check`: Pointer to boolean flag indicating if live servers are detected during check mode
## Dependencies
- Functions called/Symbols referenced:
  - [check_pghost_envvar](../c/check_pghost_envvar.md) (environment validation)
  - [find_my_exec](../f/find_my_exec.md) (executable path resolution) 
  - [last_dir_separator](../l/last_dir_separator.md) (path manipulation)
  - [canonicalize_path](../c/canonicalize_path.md) (path normalization)
  - [verify_directories](../v/verify_directories.md) (directory validation)
  - [pid_lock_file_exists](../p/pid_lock_file_exists.md) (postmaster detection)
  - [start_postmaster](start_postmaster.md) (server startup)
  - [stop_postmaster](stop_postmaster.md) (server shutdown)
- Called from:
  - [main](../m/main.md) (from pg_upgrade.c:126)

## Notes and Other Information
- Critical for ensuring upgrade environment integrity before proceeding
- Handles both normal upgrade mode and check-only mode differently
- Automatically determines binary directory from pg_upgrade executable location if not specified
- Implements sophisticated postmaster.pid handling to distinguish between running servers and stale files
- WAL replay consideration: allows recovery of committed transactions during stale pid file cleanup
- Essential safety mechanism preventing data corruption during upgrades

## Simplified Source

```c
static void setup(char *argv0, bool *live_check) {
    // Validate environment to prevent libpq connection issues
    check_pghost_envvar();

    // Set default binary directory if not specified by user
    if (!new_cluster.bindir) {
        char exec_path[MAXPGPATH];

        if (find_my_exec(argv0, exec_path) < 0)
            pg_fatal("could not find own program executable");

        // Extract directory path from executable
        *last_dir_separator(exec_path) = '\0';
        canonicalize_path(exec_path);
        new_cluster.bindir = pg_strdup(exec_path);
    }

    // Verify required directories exist
    verify_directories();

    // Check for running postmaster on old cluster
    if (pid_lock_file_exists(old_cluster.pgdata)) {
        if (start_postmaster(&old_cluster, false)) {
            // Pid file was stale, clean shutdown
            stop_postmaster(false);
        } else {
            // Server is actually running
            if (!user_opts.check)
                pg_fatal("postmaster servicing old cluster, please shutdown");
            else
                *live_check = true;  // Allow live check mode
        }
    }

    // Check for running postmaster on new cluster
    if (pid_lock_file_exists(new_cluster.pgdata)) {
        if (start_postmaster(&new_cluster, false)) {
            stop_postmaster(false);  // Clean up stale process
        } else {
            pg_fatal("postmaster servicing new cluster, please shutdown");
        }
    }
}
```