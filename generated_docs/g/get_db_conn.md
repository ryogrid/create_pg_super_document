# get_db_conn

## Location
[src/bin/pg_upgrade/server.c:57-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/server.c#L57-L91)

## Overview
Creates a PostgreSQL database connection using cluster configuration parameters and proper connection string formatting.

## Definition
```c
static PGconn *get_db_conn(ClusterInfo *cluster, const char *db_name)
```

## Detailed Description
This static function constructs a PostgreSQL connection string with properly quoted parameters and establishes a database connection. It builds the connection string using PQExpBuffer utilities to handle proper escaping of database name, username, port, and optional socket directory. The function does not perform error checking on the returned connection - this responsibility is left to the caller.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing server connection parameters (port, socket directory)
- `db_name`: Name of the target database to connect to

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendConnStrVal](../a/appendConnStrVal.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [PQconnectdb](../P/PQconnectdb.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
- Called from (representative examples):
  - [connectToServer](../c/connectToServer.md)
  - [start_postmaster](../s/start_postmaster.md)

## Notes and Other Information
- Static function - only accessible within the same source file (server.c)
- Uses PQExpBuffer for safe connection string construction with proper parameter quoting
- Includes conditional host parameter setting based on cluster->sockdir availability
- Caller is responsible for checking connection status and handling failures
- Utilizes global os_info.user for the database username
- Essential building block for pg_upgrade's database connectivity infrastructure