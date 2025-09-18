# _connParams

## Location
src/include/fe_utils/connect_utils.h: 25 - 35

## Overview
A structure that holds database connection parameters needed by PostgreSQL client programs for establishing database connections, used primarily in pg_dump and related utilities.

## Definition


## Detailed Description
The  structure encapsulates all the essential database connection parameters that PostgreSQL client applications need to establish a connection to a PostgreSQL server. This structure is particularly important in pg_dump, pg_restore, and other PostgreSQL client utilities where connection parameters need to be passed around between functions. The structure supports both simple database names and full connection strings, providing flexibility in how connections are specified.

## Parameters / Member Variables
- : Database name to connect to, which may also be a complete connection string containing multiple parameters
- : PostgreSQL server port number as a string
- : PostgreSQL server hostname or IP address
- : Username for database authentication
- : A trivalue enum (TRI_DEFAULT, TRI_NO, TRI_YES) indicating password prompting behavior
- : Optional database name that overrides the database name from the command line or connection string, affecting only the database name component

## Dependencies
- Functions called/Symbols referenced:
  - [trivalue](../t/trivalue.md) (enum type)
- Called from (representative examples):
  - Used as typedef ConnParams throughout the codebase

## Notes and Other Information
This structure is defined in src/bin/pg_dump/pg_backup.h:81-91 and serves as the foundation for connection handling in PostgreSQL client utilities. The design allows for both simple database connections and complex connection strings, making it versatile for different use cases. The  field provides a mechanism to change just the database name while preserving other connection parameters from a connection string.