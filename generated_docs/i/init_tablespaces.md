# init_tablespaces

## Location
[src/bin/pg_upgrade/tablespace.c:19-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/tablespace.c#L19-L39)

## Overview
Initializes tablespace configuration for the pg_upgrade utility by gathering tablespace paths and setting directory suffixes for both old and new clusters.

## Definition
void init_tablespaces(void)

## Detailed Description
The init_tablespaces function is a crucial initialization routine in pg_upgrade that prepares the tablespace infrastructure for database cluster upgrades. It performs three main operations:

1. Calls get_tablespace_paths() to discover and populate tablespace path information
2. Sets appropriate directory suffixes for both the old and new clusters using set_tablespace_directory_suffix()
3. Validates that the old and new clusters have different system catalog versions when tablespaces are present, preventing invalid upgrade scenarios

The function includes a critical safety check that prevents upgrades between clusters with identical system catalog versions when tablespaces are in use, as this could lead to data corruption or inconsistent states.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [get_tablespace_paths](../g/get_tablespace_paths.md)
  - [set_tablespace_directory_suffix](../s/set_tablespace_directory_suffix.md)
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [check_and_dump_old_cluster](../c/check_and_dump_old_cluster.md) (src/bin/pg_upgrade/check.c:590)

## Notes and Other Information
- This function is specific to the pg_upgrade utility and is not used in the main PostgreSQL server
- The function performs a fatal error exit if it detects an attempt to upgrade between clusters with the same system catalog version when tablespaces are present
- This initialization must be called early in the upgrade process before any tablespace-related operations
- The function modifies global cluster configuration structures (old_cluster and new_cluster)