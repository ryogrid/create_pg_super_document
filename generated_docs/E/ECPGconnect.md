# ECPGconnect

## Location
[src/interfaces/ecpg/ecpglib/connect.c:260-677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/connect.c#L260-L677)

## Overview
ECPGconnect establishes a connection to a PostgreSQL database with extensive parameter parsing, connection string handling, and connection management for ECPG embedded SQL applications.

## Definition
```c
bool ECPGconnect(int lineno, int c, const char *name, const char *user, const char *passwd, const char *connection_name, int autocommit)
```

## Detailed Description
ECPGconnect is the primary connection establishment function in ECPG that handles the complexities of PostgreSQL database connections. It supports multiple connection string formats including traditional "dbname@host:port" syntax and modern PostgreSQL URI format "postgresql://host:port/database?options". The function performs extensive parsing of connection parameters, manages the global connection list with thread safety, handles Informix compatibility mode with PG_DBPATH environment variable support, and sets up proper error handling via notice receivers. It maintains connection state in a linked list and uses pthread-specific storage for per-thread connection management.

## Parameters / Member Variables
- `lineno`: Source code line number for error reporting and debugging purposes
- `c`: Compatibility mode setting (COMPAT_MODE enum) affecting connection behavior
- `name`: Database connection string in various supported formats (traditional or URI)
- `user`: Username for database authentication (optional)
- `passwd`: Password for database authentication (optional)  
- `connection_name`: Named identifier for this connection to allow multiple connections (optional, defaults to "DEFAULT")
- `autocommit`: Boolean flag to enable/disable automatic transaction commits

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca
  - ecpg_strdup
  - ecpg_get_connection
  - ecpg_alloc
  - [PQconnectdbParams](../P/PQconnectdbParams.md)
  - [PQsetNoticeReceiver](../P/PQsetNoticeReceiver.md)
  - [ECPGnoticeReceiver](ECPGnoticeReceiver.md)
  - [pthread_mutex_lock](../p/pthread_mutex_lock.md)/unlock
  - pthread_setspecific
- Called from (representative examples):
  - ECPG-generated code for database connections
  - Test programs and applications using ECPG

## Notes and Other Information
- Returns true on successful connection, false on failure
- Supports both old-style "dbname@host:port" and new-style "postgresql://" connection strings
- Handles Informix compatibility mode with PG_DBPATH environment variable
- Thread-safe implementation with mutex protection for connection list management
- Automatically registers ECPGnoticeReceiver for handling PostgreSQL notices and warnings
- Performs extensive memory management and cleanup on error conditions
- Part of the core ECPG infrastructure for embedded SQL in C applications
- Connection parameters are passed to libpq via keyword-value arrays