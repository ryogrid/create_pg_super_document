# connectToServer

## Location
src/bin/pg_upgrade/server.c: 28 - 56

## Overview
Connects to a designated PostgreSQL server database with error handling and automatic program termination on connection failure.

## Definition
```c
PGconn *connectToServer(ClusterInfo *cluster, const char *db_name)
```

## Detailed Description
This function establishes a connection to a specific database on a PostgreSQL server using cluster information. It serves as a wrapper around `get_db_conn` with built-in error handling. If the connection attempt fails or returns an invalid status, the function logs the error message, cleans up any partial connections, displays a failure message, and terminates the program with exit code 1. Upon successful connection, it also executes a security query to set a secure search path.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing server connection details
- `db_name`: Name of the target database to connect to

## Dependencies
- Functions called/Symbols referenced:
  - get_db_conn
  - PQstatus
  - pg_log
  - PQfinish
  - executeQueryOrDie
  - PQclear
- Called from (representative examples):
  - check_for_data_types_usage
  - check_is_install_user
  - check_proper_datallowconn
  - get_loadable_libraries
  - get_template0_info
  - get_db_infos

## Notes and Other Information
- This function implements a "fail-fast" approach - any connection failure results in immediate program termination
- Automatically sets a secure search path using ALWAYS_SECURE_SEARCH_PATH_SQL after successful connection
- Used extensively throughout pg_upgrade for various database checks and operations
- Part of the PostgreSQL upgrade utility's server connection infrastructure