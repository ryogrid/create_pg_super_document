# check_bin_dir

## Location
[src/bin/pg_upgrade/exec.c:383-428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/exec.c#L383-L428)

## Overview
Validates a PostgreSQL cluster's binary directory by verifying the presence and accessibility of required executable files needed for the pg_upgrade process.

## Definition

```c
struct stat statBuf;
```
## Detailed Description
This function performs comprehensive validation of a PostgreSQL cluster's binary directory structure and executables. It first verifies that the binary directory exists and is actually a directory, then checks for the presence of essential PostgreSQL executables required for the upgrade process.

The function handles version-specific executable names (like pg_resetxlog renamed to pg_resetwal in PostgreSQL 10) and conditionally checks for additional executables when validating the target cluster. When check_versions is true, it also validates that the binary versions match the expected pg_upgrade version, which is crucial for target cluster validation.

For the new target cluster, additional utilities like initdb, pg_dump, pg_dumpall, pg_restore, psql, and vacuumdb are also validated since they are required for the upgrade process.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing cluster configuration including bindir path and version information
- `check_versions`: Boolean flag indicating whether to verify that binary versions match the expected pg_upgrade version

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md)
  - report_status
  - S_ISDIR
  - [check_exec](check_exec.md)
  - [get_bin_version](../g/get_bin_version.md)
  - GET_MAJOR_VERSION
  - PG_FATAL
- Called from (representative examples):
  - [verify_directories](../v/verify_directories.md)

## Notes and Other Information
- Exits the program with a fatal error if the binary directory is missing or not accessible
- Handles PostgreSQL version-specific binary naming changes (v10+ renames)
- Performs additional validation for target cluster binaries (new_cluster)
- Version checking is typically enabled for target cluster validation but disabled for source cluster
- Essential for ensuring all required executables are available before starting the upgrade process

## Simplified Source

```c
static void check_bin_dir(ClusterInfo *cluster, bool check_versions) {
    struct stat statBuf;

    // Verify binary directory exists and is accessible
    if (stat(cluster->bindir, &statBuf) != 0)
        report_status(PG_FATAL, "check for \"%s\" failed: %m", cluster->bindir);
    else if (!S_ISDIR(statBuf.st_mode))
        report_status(PG_FATAL, "\"%s\" is not a directory", cluster->bindir);

    // Check core PostgreSQL executables required for all clusters
    check_exec(cluster->bindir, "postgres", check_versions);
    check_exec(cluster->bindir, "pg_controldata", check_versions);
    check_exec(cluster->bindir, "pg_ctl", check_versions);

    // Get binary version after verifying pg_ctl exists
    get_bin_version(cluster);

    // Check reset utility (name changed in v10)
    if (GET_MAJOR_VERSION(cluster->bin_version) <= 906)
        check_exec(cluster->bindir, "pg_resetxlog", check_versions);  // Pre-v10
    else
        check_exec(cluster->bindir, "pg_resetwal", check_versions);   // v10+

    // Additional executables needed only for target cluster
    if (cluster == &new_cluster) {
        check_exec(cluster->bindir, "initdb", check_versions);
        check_exec(cluster->bindir, "pg_dump", check_versions);
        check_exec(cluster->bindir, "pg_dumpall", check_versions);
        check_exec(cluster->bindir, "pg_restore", check_versions);
        check_exec(cluster->bindir, "psql", check_versions);
        check_exec(cluster->bindir, "vacuumdb", check_versions);
    }
}
```