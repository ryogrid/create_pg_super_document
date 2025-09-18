# check_bin_dir

## Location
src/bin/pg_upgrade/exec.c: 383 - 428

## Overview
Validates a PostgreSQL cluster's binary directory by verifying the presence and accessibility of required executable files needed for the pg_upgrade process.

## Definition


## Detailed Description
This function performs comprehensive validation of a PostgreSQL cluster's binary directory structure and executables. It first verifies that the binary directory exists and is actually a directory, then checks for the presence of essential PostgreSQL executables required for the upgrade process.

The function handles version-specific executable names (like pg_resetxlog renamed to pg_resetwal in PostgreSQL 10) and conditionally checks for additional executables when validating the target cluster. When check_versions is true, it also validates that the binary versions match the expected pg_upgrade version, which is crucial for target cluster validation.

For the new target cluster, additional utilities like initdb, pg_dump, pg_dumpall, pg_restore, psql, and vacuumdb are also validated since they are required for the upgrade process.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing cluster configuration including bindir path and version information
- `check_versions`: Boolean flag indicating whether to verify that binary versions match the expected pg_upgrade version

## Dependencies
- Functions called/Symbols referenced:
  - stat
  - report_status
  - S_ISDIR
  - check_exec
  - get_bin_version
  - GET_MAJOR_VERSION
  - PG_FATAL
- Called from (representative examples):
  - verify_directories

## Notes and Other Information
- Exits the program with a fatal error if the binary directory is missing or not accessible
- Handles PostgreSQL version-specific binary naming changes (v10+ renames)
- Performs additional validation for target cluster binaries (new_cluster)
- Version checking is typically enabled for target cluster validation but disabled for source cluster
- Essential for ensuring all required executables are available before starting the upgrade process