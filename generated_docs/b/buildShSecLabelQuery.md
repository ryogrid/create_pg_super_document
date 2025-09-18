# buildShSecLabelQuery

## Location
[src/bin/pg_dump/dumputils.c:675-692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L675-L692)

## Overview
Builds a SQL query to retrieve security labels for shared database objects from the pg_shseclabel system catalog.

## Definition
void buildShSecLabelQuery(const char *catalog_name, Oid objectId, PQExpBuffer sql)

## Detailed Description
This function constructs a SQL query that retrieves security labels (provider and label pairs) for shared database objects. Shared objects are those that exist at the cluster level rather than within individual databases, such as databases, tablespaces, and roles. The function appends the query to the provided PQExpBuffer without executing it, allowing the caller to handle SQL execution and error management. The query targets the pg_shseclabel system catalog, which stores security labels for shared objects.

## Parameters / Member Variables
- `catalog_name`: Name of the system catalog containing the object (e.g., "pg_database")
- `objectId`: OID of the object for which to retrieve security labels
- `sql`: PQExpBuffer to which the constructed query will be appended

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
- Called from (representative examples):
  - [dumpDatabase](../d/dumpDatabase.md) (src/bin/pg_dump/pg_dump.c:3333)
  - [buildShSecLabels](buildShSecLabels.md) (src/bin/pg_dump/pg_dumpall.c:1738)

## Notes and Other Information
- Used specifically for shared objects that exist at the PostgreSQL cluster level
- The function does not execute the query to keep the file free of SQL error handling assumptions
- Part of PostgreSQLs security label infrastructure for mandatory access control systems
- The constructed query uses regclass casting for type safety when referencing catalog names
- Essential for preserving security labels during database dump and restore operations