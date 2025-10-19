# connectDatabase

## Location
[src/bin/pg_dump/pg_dumpall.c:1757-1945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L1757-L1945)

## Overview
Establishes a PostgreSQL database connection with comprehensive parameter handling, password prompting, version compatibility checking, and connection string management.

## Definition

```c
structConnStr(keywords, values);
```
## Detailed Description
This function provides a robust connection establishment mechanism for PostgreSQL client utilities, particularly pg_dumpall. It handles complex connection parameter merging from multiple sources (connection strings, individual parameters), implements interactive password prompting with retry logic, and performs comprehensive connection validation.

The function merges connection parameters from a connection string with individual parameters (host, port, user, etc.), explicitly filtering out any dbname from the connection string to avoid conflicts. It supports automatic password prompting when needed, maintains password state across retry attempts, and validates server version compatibility to ensure proper operation.

After establishing the connection, it constructs and stores a canonical connection string for later use, performs server version validation against supported ranges (9.2+ to current major version), and executes security-related initialization queries before returning the connection.

## Parameters / Member Variables
- : Target database name to connect to
- : Optional connection string with additional parameters
- : PostgreSQL server hostname or address
- : PostgreSQL server port number
- : Username for authentication
- : Tristate value controlling password prompting behavior (TRI_YES/TRI_NO/TRI_DEFAULT)
- : If true, function exits on connection failure; if false, returns NULL

## Dependencies
- Functions called/Symbols referenced:
  - [trivalue](../t/trivalue.md) (enum type for tristate values)
  - [PQconninfoOption](../P/PQconninfoOption.md) (PostgreSQL connection info structure)
  - [TRI_YES](../T/TRI_YES.md)/TRI_NO (tristate constants)
  - [simple_prompt](../s/simple_prompt.md) (password prompting utility)
  - [PQconninfoFree](../P/PQconninfoFree.md) (connection info cleanup)
  - [PQconninfoParse](../P/PQconninfoParse.md) (connection string parsing)
  - [pg_malloc0](../p/pg_malloc0.md) (memory allocation)
  - [PQconnectdbParams](../P/PQconnectdbParams.md) (PostgreSQL connection establishment)
  - [PQstatus](../P/PQstatus.md)/CONNECTION_BAD (connection status checking)
  - [PQconnectionNeedsPassword](../P/PQconnectionNeedsPassword.md) (password requirement checking)
  - [PQfinish](../P/PQfinish.md) (connection cleanup)
  - [constructConnStr](constructConnStr.md) (connection string construction)
  - [PQparameterStatus](../P/PQparameterStatus.md)/PQserverVersion (version checking)
  - [executeQuery](../e/executeQuery.md)/ALWAYS_SECURE_SEARCH_PATH_SQL (security initialization)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dumpall.c at multiple lines)
  - [connectMaintenanceDatabase](connectMaintenanceDatabase.md) (in connect_utils.c)
  - Various database utilities (pg_amcheck, clusterdb, reindexdb, vacuumdb)

## Notes and Other Information
- Sets global 'connstr' variable with the successful connection string
- Maintains static password storage for retry attempts across calls
- Supports server versions from 9.2 up to the current major version
- Implements robust error handling with optional graceful failure mode
- Executes security initialization query (ALWAYS_SECURE_SEARCH_PATH_SQL) after connection
- Memory management handled carefully with proper cleanup on all exit paths
- Used widely across PostgreSQL client utilities for consistent connection handling
- Password prompting preserves entered passwords for subsequent connection attempts

## Simplified Source

```c
static PGconn *connectDatabase(const char *dbname, const char *connection_string,
                              const char *pghost, const char *pgport, const char *pguser,
                              trivalue prompt_password, bool fail_on_error)
{
    PGconn *conn;
    bool new_pass;
    const char *remoteversion_str;
    int my_version;
    const char **keywords = NULL;
    const char **values = NULL;
    PQconninfoOption *conn_opts = NULL;
    static char *password = NULL;

    // Prompt for password if explicitly requested
    if (prompt_password == TRI_YES && !password)
        password = simple_prompt("Password: ", false);

    // Connection retry loop for password authentication
    do
    {
        int argcount = 6;
        PQconninfoOption *conn_opt;
        char *err_msg = NULL;
        int i = 0;

        // Cleanup from previous attempts
        free(keywords);
        free(values);
        PQconninfoFree(conn_opts);

        // Parse connection string and merge with individual parameters
        if (connection_string)
        {
            conn_opts = PQconninfoParse(connection_string, &err_msg);
            if (conn_opts == NULL)
                pg_fatal("%s", err_msg);

            // Count valid parameters (excluding dbname)
            for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
            {
                if (conn_opt->val != NULL && conn_opt->val[0] != '\0' &&
                    strcmp(conn_opt->keyword, "dbname") != 0)
                    argcount++;
            }
        }

        // Allocate arrays for connection parameters
        keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));
        values = pg_malloc0((argcount + 1) * sizeof(*values));

        // Copy connection string parameters (excluding dbname)
        if (connection_string)
        {
            for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
            {
                if (conn_opt->val != NULL && conn_opt->val[0] != '\0' &&
                    strcmp(conn_opt->keyword, "dbname") != 0)
                {
                    keywords[i] = conn_opt->keyword;
                    values[i] = conn_opt->val;
                    i++;
                }
            }
        }

        // Add individual connection parameters
        if (pghost) { keywords[i] = "host"; values[i] = pghost; i++; }
        if (pgport) { keywords[i] = "port"; values[i] = pgport; i++; }
        if (pguser) { keywords[i] = "user"; values[i] = pguser; i++; }
        if (password) { keywords[i] = "password"; values[i] = password; i++; }
        if (dbname) { keywords[i] = "dbname"; values[i] = dbname; i++; }
        keywords[i] = "fallback_application_name"; values[i] = progname; i++;

        // Attempt connection
        new_pass = false;
        conn = PQconnectdbParams(keywords, values, true);

        if (!conn)
            pg_fatal("could not connect to database \"%s\"", dbname);

        // Check if password is needed
        if (PQstatus(conn) == CONNECTION_BAD &&
            PQconnectionNeedsPassword(conn) &&
            !password &&
            prompt_password != TRI_NO)
        {
            PQfinish(conn);
            password = simple_prompt("Password: ", false);
            new_pass = true;
        }
    } while (new_pass);

    // Handle connection failure
    if (PQstatus(conn) == CONNECTION_BAD)
    {
        if (fail_on_error)
            pg_fatal("%s", PQerrorMessage(conn));
        else
        {
            PQfinish(conn);
            // Cleanup and return NULL
            free(keywords);
            free(values);
            PQconninfoFree(conn_opts);
            return NULL;
        }
    }

    // Store connection string for later use
    connstr = constructConnStr(keywords, values);

    // Cleanup parameter arrays
    free(keywords);
    free(values);
    PQconninfoFree(conn_opts);

    // Version compatibility check
    remoteversion_str = PQparameterStatus(conn, "server_version");
    if (!remoteversion_str)
        pg_fatal("could not get server version");
    server_version = PQserverVersion(conn);
    if (server_version == 0)
        pg_fatal("could not parse server version \"%s\"", remoteversion_str);

    my_version = PG_VERSION_NUM;

    // Validate version compatibility (9.2+ to current major version)
    if (my_version != server_version &&
        (server_version < 90200 ||
         (server_version / 100) > (my_version / 100)))
    {
        pg_log_error("aborting because of server version mismatch");
        pg_log_error_detail("server version: %s; %s version: %s",
                           remoteversion_str, progname, PG_VERSION);
        exit_nicely(1);
    }

    // Execute security initialization
    PQclear(executeQuery(conn, ALWAYS_SECURE_SEARCH_PATH_SQL));

    return conn;
}
```