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
  - [ecpg_strdup](../e/ecpg_strdup.md)
  - [ecpg_get_connection](../e/ecpg_get_connection.md)
  - [ecpg_alloc](../e/ecpg_alloc.md)
  - [PQconnectdbParams](../P/PQconnectdbParams.md)
  - [PQsetNoticeReceiver](../P/PQsetNoticeReceiver.md)
  - [ECPGnoticeReceiver](ECPGnoticeReceiver.md)
  - [pthread_mutex_lock](../p/pthread_mutex_lock.md)/unlock
  - [pthread_setspecific](../p/pthread_setspecific.md)
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

## Simplified Source

```c
// Simplified version of ECPGconnect
bool ECPGconnect(int lineno, int c, const char *name, const char *user,
                const char *passwd, const char *connection_name, int autocommit) {
    struct sqlca_t *sqlca = ECPGget_sqlca();
    struct connection *this;
    char *dbname = name ? ecpg_strdup(name, lineno) : NULL;
    char *host = NULL, *port = NULL, *realname = NULL, *options = NULL;
    const char **conn_keywords, **conn_values;
    int i, connect_params = 0;

    // Initialize SQLCA and auto memory
    if (sqlca == NULL) {
        ecpg_raise(lineno, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
        ecpg_free(dbname);
        return false;
    }
    ecpg_init_sqlca(sqlca);
    ecpg_clear_auto_mem();

    // Handle Informix compatibility mode
    if (INFORMIX_MODE(c)) {
        char *envname = getenv("PG_DBPATH");
        if (envname) {
            ecpg_free(dbname);
            dbname = ecpg_strdup(envname, lineno);
        }
    }

    // Set default connection name
    if (dbname == NULL && connection_name == NULL)
        connection_name = "DEFAULT";

    // Check for duplicate connection name
    if (ecpg_get_connection(connection_name)) {
        ecpg_free(dbname);
        ecpg_log("ECPGconnect: connection identifier %s is already in use\\n", connection_name);
        return false;
    }

    // Allocate connection structure
    this = (struct connection *) ecpg_alloc(sizeof(struct connection), lineno);
    if (this == NULL) {
        ecpg_free(dbname);
        return false;
    }

    // Parse connection string formats
    if (dbname != NULL) {
        // Handle postgresql:// URI format
        if (strncmp(dbname, "tcp:", 4) == 0 || strncmp(dbname, "unix:", 5) == 0) {
            // Parse modern URI format: tcp:postgresql://host:port/db?options
            // [Complex URI parsing logic simplified]
            realname = extract_database_name(dbname);
            host = extract_host(dbname);
            port = extract_port(dbname);
            options = extract_options(dbname);
        } else {
            // Handle old format: dbname@host:port
            // [Old format parsing logic simplified]
            realname = extract_dbname_old_format(dbname);
            host = extract_host_old_format(dbname);
            port = extract_port_old_format(dbname);
        }
    }

    // Count and prepare connection parameters
    connect_params = count_connection_params(realname, host, port, user, passwd, options);

    // Allocate parameter arrays
    conn_keywords = (const char **) ecpg_alloc((connect_params + 1) * sizeof(char *), lineno);
    conn_values = (const char **) ecpg_alloc(connect_params * sizeof(char *), lineno);
    if (conn_keywords == NULL || conn_values == NULL) {
        cleanup_and_return_false(host, port, options, realname, dbname, conn_keywords, conn_values, this);
        return false;
    }

    // Add connection to global list with thread safety
    pthread_mutex_lock(&connections_mutex);

    this->name = ecpg_strdup(connection_name ? connection_name : realname, lineno);
    this->cache_head = NULL;
    this->prep_stmts = NULL;
    this->next = all_connections;
    all_connections = this;
    pthread_setspecific(actual_connection_key, all_connections);
    actual_connection = all_connections;

    // Build connection parameter arrays
    i = 0;
    if (realname) { conn_keywords[i] = "dbname"; conn_values[i] = realname; i++; }
    if (host) { conn_keywords[i] = "host"; conn_values[i] = host; i++; }
    if (port) { conn_keywords[i] = "port"; conn_values[i] = port; i++; }
    if (user && strlen(user) > 0) { conn_keywords[i] = "user"; conn_values[i] = user; i++; }
    if (passwd && strlen(passwd) > 0) { conn_keywords[i] = "password"; conn_values[i] = passwd; i++; }

    // Parse and add options
    if (options) {
        parse_options_string(options, conn_keywords, conn_values, &i);
    }

    conn_keywords[i] = NULL;  // terminator

    // Establish actual connection
    this->connection = PQconnectdbParams(conn_keywords, conn_values, 0);

    // Cleanup parameter memory
    cleanup_connection_params(host, port, options, dbname, conn_values, conn_keywords);

    // Check connection status
    if (PQstatus(this->connection) == CONNECTION_BAD) {
        const char *errmsg = PQerrorMessage(this->connection);
        const char *db = realname ? realname : ecpg_gettext("<DEFAULT>");

        ecpg_log("ECPGconnect: %s", errmsg);
        ecpg_finish(this);
        pthread_mutex_unlock(&connections_mutex);
        ecpg_raise(lineno, ECPG_CONNECT, ECPG_SQLSTATE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, db);
        if (realname) ecpg_free(realname);
        return false;
    }

    if (realname) ecpg_free(realname);
    pthread_mutex_unlock(&connections_mutex);

    // Configure connection
    this->autocommit = autocommit;
    PQsetNoticeReceiver(this->connection, &ECPGnoticeReceiver, (void *) this);

    return true;
}
```

Key simplifications made:
- Abstracted complex connection string parsing into helper functions
- Simplified URI and old-format parsing logic
- Reduced detailed error handling paths to essential checks
- Condensed parameter array building logic
- Focused on core connection establishment flow
- Maintained thread safety and essential cleanup operations