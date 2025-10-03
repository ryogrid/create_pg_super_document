# check_hard_link

## Location
[src/bin/pg_upgrade/file.c:437-451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/file.c#L437-L451)

## Overview
Tests the ability to create hard links between the old and new data directories during pg_upgrade operations in link mode.

## Definition

```c
void
check_hard_link(void)
```
## Detailed Description
This function is a critical component of pg_upgrade's link mode functionality that verifies whether hard links can be created between files in the old and new PostgreSQL data directories. Link mode is an optimization that allows pg_upgrade to create hard links to existing files instead of copying them, which significantly reduces upgrade time and disk space usage for large databases.

The function performs a practical test by:
1. Constructing file paths for the existing PG_VERSION file in the old cluster and a test link file in the new cluster
2. Removing any existing test link file to ensure a clean test
3. Attempting to create a hard link from the old cluster's PG_VERSION to the new cluster
4. If successful, removing the test link file to clean up

Hard links can only be created between files on the same filesystem, so this test effectively validates that both data directories reside on the same filesystem - a requirement for link mode operation.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - unlink (system call for removing files)
  - [link](../l/link.md) (system call for creating hard links)
  - snprintf (for formatting file paths)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting function)
- Global variables accessed:
  - old_cluster.pgdata (source data directory path)
  - new_cluster.pgdata (destination data directory path)
- Called from:
  - [check_new_cluster](check_new_cluster.md) (src/bin/pg_upgrade/check.c:705)

## Notes and Other Information
- This function is essential for validating link mode compatibility in pg_upgrade
- Hard links require both directories to be on the same filesystem/mount point
- The test uses PG_VERSION file as it's guaranteed to exist and is small
- Failure indicates that link mode cannot be used, forcing pg_upgrade to fall back to copy mode
- The temporary test link file uses '.linktest' suffix for identification
- Link mode can provide significant performance benefits for large database upgrades
- The error message explicitly explains the filesystem requirement to help users diagnose the issue
- Cleanup is performed regardless of test outcome to prevent leftover files