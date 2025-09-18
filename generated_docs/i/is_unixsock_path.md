# is_unixsock_path

## Location
[src/include/libpq/pqcomm.h:67-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqcomm.h#L67-L86)

## Overview
A static inline function that determines whether a given path string represents a Unix domain socket address by checking if it's an absolute path or starts with the '@' character.

## Definition


## Detailed Description
This function provides a simple test to identify Unix domain socket paths in PostgreSQL's connection handling code. It recognizes two forms of Unix socket addresses:
1. Absolute filesystem paths (e.g., "/tmp/.s.PGSQL.5432")
2. Abstract socket names starting with '@' (Linux-specific feature)

The function is implemented as a static inline for performance efficiency since it's a simple check that may be called frequently during connection establishment.

## Parameters / Member Variables
- `path`: A null-terminated string representing a potential socket path to be tested

## Dependencies
- Functions called/Symbols referenced:
  - is_absolute_path
- Called from (representative examples):
  - [check_pghost_envvar](../c/check_pghost_envvar.md) (in pg_upgrade)
  - [exec_command_conninfo](../e/exec_command_conninfo.md) (in psql)
  - [do_connect](../d/do_connect.md) (in psql)
  - pqConnectOptions2 (in libpq)
  - [passwordFromFile](../p/passwordFromFile.md) (in libpq)

## Notes and Other Information
- The '@' prefix for abstract sockets is a Linux-specific feature that allows socket names to exist in an abstract namespace rather than the filesystem
- This function is defined in the header file pqcomm.h, making it available throughout the PostgreSQL codebase for connection-related operations
- The function helps distinguish between TCP/IP hostnames and Unix socket paths in connection strings