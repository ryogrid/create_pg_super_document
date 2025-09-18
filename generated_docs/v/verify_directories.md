# verify_directories

## Location
src/bin/pg_upgrade/exec.c: 263 - 287

## Overview
This function performs comprehensive verification of directories and executables for both old and new PostgreSQL server clusters during the upgrade process.

## Definition


## Detailed Description
The  function is a critical component of the pg_upgrade process that validates the accessibility and integrity of PostgreSQL installations. It first checks that the current working directory has proper read, write, and execute permissions (using platform-specific methods). Then it systematically verifies both the old and new cluster installations by checking their binary directories and data directories. The function uses different verification approaches for the old and new clusters - the old cluster is checked without creating new binaries (false parameter), while the new cluster may require additional binary verification (true parameter).

## Parameters / Member Variables
This function takes no parameters and operates on global cluster structures (old_cluster and new_cluster).

## Dependencies
- Functions called/Symbols referenced:
  - access (Unix/Linux)
  - [win32_check_directory_write_permissions](../w/win32_check_directory_write_permissions.md) (Windows)
  - [check_bin_dir](../c/check_bin_dir.md) (called twice, for old and new clusters)
  - [check_data_dir](../c/check_data_dir.md) (called twice, for old and new clusters)
  - [pg_fatal](../p/pg_fatal.md) (for fatal error reporting)
- Called from (representative examples):
  - [setup](../s/setup.md) (in pg_upgrade.c)

## Notes and Other Information
- This function has platform-specific behavior: uses access() on Unix/Linux and win32_check_directory_write_permissions() on Windows
- The function operates on global variables old_cluster and new_cluster
- The boolean parameter passed to check_bin_dir indicates whether this is verification for the new cluster (true) or old cluster (false)
- Essential for ensuring upgrade prerequisites are met before attempting data migration
- May update parameter values as noted in the function comment, though the function signature suggests it works with global state
- Part of the initialization sequence in PostgreSQL major version upgrades