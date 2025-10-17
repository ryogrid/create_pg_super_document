# GetConnection

## Location
[src/bin/pg_basebackup/streamutil.c:63-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L63-L281)

## Overview
Establishes a connection to PostgreSQL server using provided connection parameters, with support for replication connections and automatic password handling.

## Definition

```c
struct a umask
	 * for creating directories and files.
	 */
	if (!RetrieveDataDirCreatePerm(tmpconn))
	{
		PQfinish(tmpconn);
		exit(1);
	}

	return tmpconn;
```
## Detailed Description
The GetConnection function creates a PostgreSQL database connection with specialized handling for replication connections used by pg_basebackup utilities. It merges connection parameters from connection strings and individual options, handles password prompts when needed, and performs security validations including setting a secure search path and verifying integer_datetimes compatibility. The function automatically retries connection attempts when password authentication is required and performs essential security checks before returning a valid connection.

## Parameters / Member Variables
- No parameters (uses global variables: `connstr`, `pghost`, `pgport`, `username`, `promptPassword`, `progname`, `replication_slot`, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [PQconninfoParse](../P/PQconninfoParse.md)
  - [PQconnectdbParams](../P/PQconnectdbParams.md)  
  - [PQstatus](../P/PQstatus.md)
  - [PQconnectionNeedsPassword](../P/PQconnectionNeedsPassword.md)
  - [PQfinish](../P/PQfinish.md)
  - [PQexec](../P/PQexec.md)
  - [PQparameterStatus](../P/PQparameterStatus.md)
  - [PQserverVersion](../P/PQserverVersion.md)
  - [RetrieveDataDirCreatePerm](../R/RetrieveDataDirCreatePerm.md)
  - [simple_prompt](../s/simple_prompt.md)
  - [pg_malloc0](../p/pg_malloc0.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_basebackup.c, pg_receivewal.c, pg_recvlogical.c)
  - [StartLogStreamer](../S/StartLogStreamer.md)
  - [StreamLog](../S/StreamLog.md)
  - [StreamLogicalLog](../S/StreamLogicalLog.md)
  - [setup_connection](../s/setup_connection.md) (in pg_dump.c)

## Notes and Other Information
- Returns NULL on non-permanent errors, calls exit(1) on permanent errors
- Automatically sets dbname to "replication" for replication connections
- Sets secure search path for PostgreSQL 10+ servers when using database connections
- Validates integer_datetimes compatibility between client and server
- Retrieves and configures data directory permissions via RetrieveDataDirCreatePerm
- Supports both connection string format and individual parameter specification

## Simplified Source

```c
PGconn *
GetConnection(void)
{
    PGconn *tmpconn;
    int argcount = 7;
    const char **keywords;
    const char **values;
    bool need_password;
    PQconninfoOption *conn_opts = NULL;
    char *err_msg = NULL;

    // Parse connection string or use individual parameters
    int i = 0;
    if (connection_string) {
        conn_opts = PQconninfoParse(connection_string, &err_msg);
        if (conn_opts == NULL)
            pg_fatal("%s", err_msg);

        // Count parameters in connection string
        for (PQconninfoOption *conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++) {
            if (conn_opt->val != NULL && conn_opt->val[0] != '\0')
                argcount++;
        }
    }

    // Allocate parameter arrays
    keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));
    values = pg_malloc0((argcount + 1) * sizeof(*values));

    // Set default parameters
    keywords[i] = "dbname";
    values[i] = (dbname == NULL) ? "replication" : dbname;
    i++;

    keywords[i] = "replication";
    values[i] = (dbname == NULL) ? "true" : "database";
    i++;

    keywords[i] = "fallback_application_name";
    values[i] = progname;
    i++;

    // Add connection parameters if provided
    if (dbhost) {
        keywords[i] = "host";
        values[i] = dbhost;
        i++;
    }
    if (dbuser) {
        keywords[i] = "user";
        values[i] = dbuser;
        i++;
    }
    if (dbport) {
        keywords[i] = "port";
        values[i] = dbport;
        i++;
    }

    // Handle password authentication
    need_password = (dbgetpassword == 1 && !password);

    do {
        if (need_password) {
            free(password);
            password = simple_prompt("Password: ", false);
            need_password = false;
        }

        // Set password parameter if available
        if (password) {
            keywords[i] = "password";
            values[i] = password;
        }

        // Attempt connection
        tmpconn = PQconnectdbParams(keywords, values, !connection_string);

        if (!tmpconn)
            pg_fatal("could not connect to server");

        // Check if password is needed
        if (PQstatus(tmpconn) == CONNECTION_BAD &&
            PQconnectionNeedsPassword(tmpconn) &&
            dbgetpassword != -1) {
            PQfinish(tmpconn);
            need_password = true;
        }
    } while (need_password);

    // Check connection status
    if (PQstatus(tmpconn) != CONNECTION_OK) {
        pg_log_error("%s", PQerrorMessage(tmpconn));
        PQfinish(tmpconn);
        // Cleanup
        free(values);
        free(keywords);
        PQconninfoFree(conn_opts);
        return NULL;
    }

    // Cleanup parameter arrays
    free(values);
    free(keywords);
    PQconninfoFree(conn_opts);

    // Set secure search path for PostgreSQL 10+
    if (dbname != NULL && PQserverVersion(tmpconn) >= 100000) {
        PGresult *res = PQexec(tmpconn, ALWAYS_SECURE_SEARCH_PATH_SQL);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            pg_log_error("could not clear \"search_path\": %s",
                         PQerrorMessage(tmpconn));
            PQclear(res);
            PQfinish(tmpconn);
            exit(1);
        }
        PQclear(res);
    }

    // Verify integer_datetimes compatibility
    const char *tmpparam = PQparameterStatus(tmpconn, "integer_datetimes");
    if (!tmpparam || strcmp(tmpparam, "on") != 0) {
        pg_log_error("integer_datetimes compatibility issue");
        PQfinish(tmpconn);
        exit(1);
    }

    // Retrieve data directory permissions
    if (!RetrieveDataDirCreatePerm(tmpconn)) {
        PQfinish(tmpconn);
        exit(1);
    }

    return tmpconn;
}
```