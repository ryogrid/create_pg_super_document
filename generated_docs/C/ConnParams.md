# ConnParams

## Location
[src/bin/pg_dump/pg_backup.h:92-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup.h#L92-L93)

## Overview
A structure that encapsulates database connection parameters used throughout PostgreSQL client utilities for establishing database connections.

## Definition
```c
typedef struct _connParams
{
    /* These fields record the actual command line parameters */
    char       *dbname;         /* this may be a connstring! */
    char       *pgport;
    char       *pghost;
    char       *username;
    trivalue    promptPassword;
    /* If not NULL, this overrides the dbname obtained from command line */
    /* (but *only* the DB name, not anything else in the connstring) */
    char       *override_dbname;
} ConnParams;
```

## Detailed Description
The `ConnParams` structure is a standardized container for database connection parameters used across PostgreSQL client utilities including pg_dump, pg_restore, and various maintenance scripts. It centralizes connection information management and provides a consistent interface for database connectivity throughout the client toolkit.

## Parameters / Member Variables
- `dbname`: Database name or complete connection string - can contain full libpq connection parameters
- `pgport`: PostgreSQL server port number as a string
- `pghost`: PostgreSQL server hostname or socket directory path
- `username`: Database username for authentication
- `promptPassword`: Three-state flag (trivalue) controlling password prompting behavior
- `override_dbname`: Optional database name override that replaces only the database name portion of a connection string

## Dependencies
- Functions called/Symbols referenced:
  - [trivalue](../t/trivalue.md) enum for the promptPassword field
- Called from (representative examples):
  - [ConnectDatabase](ConnectDatabase.md) function in pg_backup_db.c
  - Various `main` functions in client utilities (pg_amcheck, clusterdb, createdb, etc.)
  - [connectDatabase](../c/connectDatabase.md) and `connectMaintenanceDatabase` in fe_utils/connect_utils.c
  - Multiple restoration and dump option structures as embedded members
  - Parallel processing utilities for connection management

## Notes and Other Information
This structure is fundamental to PostgreSQL's client utility architecture, providing a unified approach to connection parameter management. The `dbname` field's dual nature (simple database name or full connection string) offers flexibility in specifying connection details. The `override_dbname` field enables sophisticated connection string manipulation where only the database name needs to be changed while preserving other connection parameters. This structure is heavily used in parallel processing scenarios and forms the foundation for the connection utilities in fe_utils.