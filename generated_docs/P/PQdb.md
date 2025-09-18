# PQdb

## Location
src/interfaces/libpq/fe-connect.c: 7003 - 7010

## Overview
A public libpq API accessor function that returns the database name associated with a PostgreSQL connection.

## Definition


## Detailed Description
This function provides access to the database name that was used to establish the PostgreSQL connection. It's one of the basic accessor functions in libpq that allows applications to query connection properties without directly accessing the PGconn structure internals.

The function performs a simple validation check and returns a pointer to the internal database name string. This is particularly useful for applications that need to display connection information, log database operations, or make decisions based on which database they're connected to.

## Parameters / Member Variables
- `conn`: Pointer to a PostgreSQL connection object (const PGconn)

## Dependencies
- Functions called/Symbols referenced:
  - None (direct field access)
- Called from (representative examples):
  - [main](../m/main.md) (pg_amcheck)
  - [prohibit_crossdb_refs](../p/prohibit_crossdb_refs.md) (pg_dump)
  - [GetTableInfo](../G/GetTableInfo.md) (pgbench)
  - [exec_command_conninfo](../e/exec_command_conninfo.md) (psql)
  - [do_connect](../d/do_connect.md) (psql)
  - [cluster_one_database](../c/cluster_one_database.md) (clusterdb)
  - [vacuum_one_database](../v/vacuum_one_database.md) (vacuumdb)
  - [find_matching_idle_slot](../f/find_matching_idle_slot.md) (parallel_slot)

## Notes and Other Information
- This is a public libpq API function exposed to client applications
- Returns NULL if the connection pointer is NULL
- Returns a pointer to the internal dbName field - callers should not modify or free this string
- The returned string is valid for the lifetime of the connection
- Widely used throughout PostgreSQL client tools for connection introspection
- Part of the family of PGconn accessor functions (PQuser, PQhost, PQport, etc.)
- Essential for applications that work with multiple databases or need to display connection information
- Simple and efficient - direct field access with null pointer protection