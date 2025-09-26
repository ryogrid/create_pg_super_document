# getForeignServers

## Location
[src/bin/pg_dump/pg_dump.c:9752-9845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L9752-L9845)

## Overview
Reads all foreign servers from the system catalogs and returns them in a structured format for pg_dump processing.

## Definition
```c
ForeignServerInfo *getForeignServers(Archive *fout, int *numForeignServers)
```

## Detailed Description
This function is part of the pg_dump utility and extracts complete information about foreign servers from the PostgreSQL system catalog `pg_foreign_server`. It constructs a comprehensive SQL query to retrieve all relevant server metadata including owner, associated foreign data wrapper, server type, version, access control lists, and connection options. The function processes the query results and creates an array of `ForeignServerInfo` structures, each containing all necessary information for dumping and restoring foreign servers. Each server is assigned a dump ID and evaluated for dumpability. Foreign servers automatically include user mapping components since they can have associated user mappings.

## Parameters / Member Variables
- `fout`: Archive handle for the pg_dump operation, used for executing SQL queries
- `numForeignServers`: Output parameter that receives the count of foreign servers found

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)/PQfnumber/PQgetvalue/PQgetisnull
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [getRoleName](getRoleName.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- The function queries the `pg_foreign_server` catalog table with formatted options using `pg_options_to_table`
- Access control information uses `acldefault('S', srvowner)` to get default server privileges
- Each foreign server automatically gets `DUMP_COMPONENT_USERMAP` since servers can have user mappings
- ACL components are conditionally added based on whether ACL information exists
- The `srvfdw` field links the server to its associated foreign data wrapper via OID
- Server type and version are optional fields that may be null
- The returned `ForeignServerInfo` array must be freed by the caller