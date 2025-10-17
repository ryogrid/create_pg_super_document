# dumpDatabaseConfig

## Location
[src/bin/pg_dump/pg_dump.c:3521-3564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L3521-L3564)

## Overview
Collects and formats database-specific and role-and-database-specific SET configuration options for inclusion in database dump output.

## Definition

```c
static void
dumpDatabaseConfig(Archive *AH, PQExpBuffer outbuf,
				   const char *dbname, Oid dboid)
```
## Detailed Description
This function retrieves configuration settings that have been set at the database level or for specific role-database combinations using ALTER DATABASE SET and ALTER ROLE IN DATABASE SET commands. It queries the pg_db_role_setting system catalog to find these settings and formats them as appropriate ALTER commands for restoration. The function handles two types of configurations: database-wide settings (where setrole = 0) and role-specific settings within the database context. The generated ALTER commands are appended to the provided output buffer for inclusion in the dump.

## Parameters / Member Variables
- `*AH`: Pointer to Archive structure providing database connection and context
- `outbuf`: PQExpBuffer where the generated ALTER configuration commands will be appended
- `*dbname`: Name of the database for which to collect configuration settings
- `dboid`: OID of the database to query for configuration settings
## Dependencies
- Functions called/Symbols referenced:
  - [GetConnection](../G/GetConnection.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [makeAlterConfigCommand](../m/makeAlterConfigCommand.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
- Types referenced:
  - [Archive](../A/Archive.md)
  - PQExpBuffer
  - [PGconn](../P/PGconn.md)
  - [PGresult](../P/PGresult.md)
  - Oid
  - PGRES_TUPLES_OK
- Called from:
  - [dumpDatabase](dumpDatabase.md)

## Notes and Other Information
- Handles two distinct types of configuration settings: database-level and role-specific within database
- Uses pg_db_role_setting system catalog which stores configuration parameters set with ALTER DATABASE SET and ALTER ROLE IN DATABASE SET
- Database-level settings have setrole = 0, while role-specific settings reference actual role OIDs
- Configuration settings are stored as arrays and unnested for individual processing
- Generated ALTER commands preserve the original configuration context (DATABASE vs ROLE IN DATABASE)
- Essential for maintaining custom database behaviors and performance tunings during restore operations
- Works in conjunction with makeAlterConfigCommand to format proper SQL syntax for each configuration parameter

## Simplified Source

```c
static void dumpDatabaseConfig(Archive *AH, PQExpBuffer outbuf,
                              const char *dbname, Oid dboid) {
    PGconn *conn = GetConnection(AH);
    PQExpBuffer buf = createPQExpBuffer();
    PGresult *res;

    // First collect database-specific options (setrole = 0)
    printfPQExpBuffer(buf, "SELECT unnest(setconfig) FROM pg_db_role_setting "
                          "WHERE setrole = 0 AND setdatabase = '%u'::oid", dboid);

    res = ExecuteSqlQuery(AH, buf->data, PGRES_TUPLES_OK);

    // Generate ALTER DATABASE SET commands
    for (int i = 0; i < PQntuples(res); i++) {
        makeAlterConfigCommand(conn, PQgetvalue(res, i, 0),
                              "DATABASE", dbname, NULL, NULL, outbuf);
    }

    PQclear(res);

    // Now look for role-and-database-specific options
    printfPQExpBuffer(buf, "SELECT rolname, unnest(setconfig) "
                          "FROM pg_db_role_setting s, pg_roles r "
                          "WHERE setrole = r.oid AND setdatabase = '%u'::oid", dboid);

    res = ExecuteSqlQuery(AH, buf->data, PGRES_TUPLES_OK);

    // Generate ALTER ROLE IN DATABASE SET commands
    for (int i = 0; i < PQntuples(res); i++) {
        makeAlterConfigCommand(conn, PQgetvalue(res, i, 1),
                              "ROLE", PQgetvalue(res, i, 0),
                              "DATABASE", dbname, outbuf);
    }

    PQclear(res);
    destroyPQExpBuffer(buf);
}
```