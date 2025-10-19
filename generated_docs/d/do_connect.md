# do_connect

## Location
[src/bin/psql/command.c:3386-3849](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3386-L3849)

## Overview
Handles the \connect command in psql, establishing a new database connection with given parameters while optionally reusing parameters from the previous connection.

## Definition

```c
static bool
do_connect(enum trivalue reuse_previous_specification,
		   char *dbname, char *user, char *host, char *port)
```
## Detailed Description
The  function is the core handler for psql's \connect command, responsible for establishing database connections with specified parameters. It supports both traditional parameter-based connections and connection string formats. The function intelligently manages parameter reuse from previous connections and handles password authentication, including prompting for passwords when needed.

Key behaviors include:
- **Parameter reuse logic**: When reusing previous connection parameters, it extracts them from the current or dead connection and selectively replaces them with new values
- **Connection string support**: Parses PostgreSQL connection strings and URIs, validating that additional parameters aren't provided when using connection strings
- **Password management**: Implements sophisticated password reuse logic based on whether connection-critical parameters (user, host, port) have changed
- **Interactive vs non-interactive modes**: In interactive mode, failed connections preserve the previous connection; in scripting mode, failed connections close all connections
- **Client encoding handling**: Automatically sets client_encoding to "auto" for terminal connections without PGCLIENTENCODING

## Parameters / Member Variables
- `reuse_previous_specification`: Controls whether to reuse parameters from previous connection (TRI_YES, TRI_NO, or TRI_DEFAULT)
- `*dbname`: Target database name or connection string/URI
- `*user`: Username for authentication (can be NULL to reuse previous or use defaults)
- `*host`: Database server hostname (can be NULL to reuse previous or use defaults)
- `*port`: Database server port (can be NULL to reuse previous or use defaults)
## Dependencies
- Functions called/Symbols referenced:
  - : Identifies if dbname is a connection string
  - /: Retrieves connection parameters
  - : Parses connection strings
  - : Interactive password prompting
  - : Initiates database connection
  - : Waits for connection completion
  - : Displays connection warnings
  - : Synchronizes psql variables with new connection
- Called from (representative examples):
  - : Main \connect command handler

## Notes and Other Information
- The function implements a retry loop for password authentication, allowing users to correct failed password attempts
- Connection parameter precedence follows: command parameters > connection string values > previous connection values > libpq defaults
- Memory management is carefully handled with proper cleanup of libpq structures and locally allocated data
- The function preserves behavioral consistency between interactive and scripting modes while providing appropriate user feedback

## Simplified Source

```c
static bool do_connect(enum trivalue reuse_previous_specification,
                      char *dbname, char *user, char *host, char *port)
{
    PGconn *old_conn = pset.db;
    PGconn *new_conn = NULL;
    PQconninfoOption *conn_info;
    char *password = NULL;
    bool success = true;
    bool keep_password = true;
    bool has_connection_string = dbname ? recognized_connection_string(dbname) : false;

    // Validate parameters - don't allow extra args with connection strings
    if (has_connection_string && (user || host || port)) {
        pg_log_error("Do not give user, host, or port separately when using a connection string");
        return false;
    }

    // Determine whether to reuse previous connection parameters
    bool reuse_previous = (reuse_previous_specification == TRI_YES) ||
                         (reuse_previous_specification == TRI_DEFAULT && !has_connection_string);

    // Get connection info - either from previous connection or defaults
    if (reuse_previous) {
        if (old_conn)
            conn_info = PQconninfo(old_conn);
        else if (pset.dead_conn)
            conn_info = PQconninfo(pset.dead_conn);
        else {
            pg_log_error("No database connection exists to re-use parameters from");
            return false;
        }
    } else {
        conn_info = PQconndefaults();
    }

    if (!conn_info) {
        pg_log_error("out of memory");
        return false;
    }

    // Handle connection strings by parsing and merging parameters
    if (has_connection_string) {
        PQconninfoOption *parsed_info;
        char *errmsg;

        parsed_info = PQconninfoParse(dbname, &errmsg);
        if (parsed_info) {
            // Merge parsed connection string values into conn_info
            // Check if password reuse is still valid based on changed parameters
            for (int i = 0; conn_info[i].keyword; i++) {
                if (parsed_info[i].val) {
                    // Update connection parameter and check password implications
                    if (strcmp(parsed_info[i].keyword, "user") == 0 ||
                        strcmp(parsed_info[i].keyword, "host") == 0 ||
                        strcmp(parsed_info[i].keyword, "port") == 0) {
                        keep_password = false;  // Critical params changed
                    }
                    // Swap values to update conn_info
                    char *temp = parsed_info[i].val;
                    parsed_info[i].val = conn_info[i].val;
                    conn_info[i].val = temp;
                }
            }
            PQconninfoFree(parsed_info);
            dbname = NULL;  // Don't inject dbname separately
        } else {
            pg_log_error("%s", errmsg ? errmsg : "out of memory");
            success = false;
        }
    } else {
        // Check if individual parameters affect password reuse
        for (int i = 0; conn_info[i].keyword; i++) {
            if ((user && strcmp(conn_info[i].keyword, "user") == 0 &&
                 strcmp(user, conn_info[i].val) != 0) ||
                (host && strcmp(conn_info[i].keyword, "host") == 0 &&
                 strcmp(host, conn_info[i].val) != 0) ||
                (port && strcmp(conn_info[i].keyword, "port") == 0 &&
                 strcmp(port, conn_info[i].val) != 0)) {
                keep_password = false;
            }
        }
    }

    // Get password if needed
    if (pset.getPassword == TRI_YES && success) {
        bool canceled = false;
        password = prompt_for_password(has_connection_string ? NULL : user, &canceled);
        success = !canceled;
    }

    // Connection attempt loop
    while (success) {
        // Build parameter arrays for PQconnectStartParams
        const char **keywords = pg_malloc((MAX_CONN_PARAMS + 1) * sizeof(*keywords));
        const char **values = pg_malloc((MAX_CONN_PARAMS + 1) * sizeof(*values));
        int param_count = 0;

        // Copy connection parameters, injecting our specific values
        for (int i = 0; conn_info[i].keyword; i++) {
            keywords[param_count] = conn_info[i].keyword;

            if (dbname && strcmp(conn_info[i].keyword, "dbname") == 0)
                values[param_count] = dbname;
            else if (user && strcmp(conn_info[i].keyword, "user") == 0)
                values[param_count] = user;
            else if (host && strcmp(conn_info[i].keyword, "host") == 0)
                values[param_count] = host;
            else if (port && strcmp(conn_info[i].keyword, "port") == 0)
                values[param_count] = port;
            else if ((password || !keep_password) &&
                     strcmp(conn_info[i].keyword, "password") == 0)
                values[param_count] = password;
            else if (strcmp(conn_info[i].keyword, "fallback_application_name") == 0)
                values[param_count] = pset.progname;
            else if (conn_info[i].val)
                values[param_count] = conn_info[i].val;
            else
                continue;  // Skip unset parameters

            param_count++;
        }
        keywords[param_count] = NULL;
        values[param_count] = NULL;

        // Attempt connection
        new_conn = PQconnectStartParams(keywords, values, false);
        pg_free(keywords);
        pg_free(values);

        wait_until_connected(new_conn);

        if (PQstatus(new_conn) == CONNECTION_OK)
            break;  // Success!

        // Handle authentication retry
        if (!password && PQconnectionNeedsPassword(new_conn) && pset.getPassword != TRI_NO) {
            bool canceled = false;
            password = prompt_for_password(PQuser(new_conn), &canceled);
            PQfinish(new_conn);
            new_conn = NULL;
            success = !canceled;
            continue;
        }

        // Connection failed
        if (!new_conn)
            pg_log_error("out of memory");
        success = false;
    }

    // Cleanup
    pg_free(password);
    PQconninfoFree(conn_info);

    if (!success) {
        // Handle connection failure
        if (pset.cur_cmd_interactive) {
            // Interactive mode: keep old connection
            if (new_conn) {
                pg_log_info("%s", PQerrorMessage(new_conn));
                PQfinish(new_conn);
            }
            if (old_conn)
                pg_log_info("Previous connection kept");
        } else {
            // Scripting mode: close all connections
            if (new_conn) {
                pg_log_error("\\connect: %s", PQerrorMessage(new_conn));
                PQfinish(new_conn);
            }
            if (old_conn) {
                PQfinish(old_conn);
                pset.db = NULL;
                ResetCancelConn();
                UnsyncVariables();
            }
        }
        return false;
    }

    // Success: replace old connection with new one
    PQsetNoticeProcessor(new_conn, NoticeProcessor, NULL);
    pset.db = new_conn;
    SyncVariables();
    connection_warnings(false);

    // Display connection info to user
    if (!pset.quiet) {
        printf("You are now connected to database \"%s\" as user \"%s\" on host \"%s\" at port \"%s\".\n",
               PQdb(pset.db), PQuser(pset.db), PQhost(pset.db), PQport(pset.db));
    }

    // Cleanup old connections
    if (old_conn)
        PQfinish(old_conn);
    if (pset.dead_conn) {
        PQfinish(pset.dead_conn);
        pset.dead_conn = NULL;
    }

    return true;
}
```

**Simplified Logic:**
1. **Parameter Validation**: Check for invalid parameter combinations with connection strings
2. **Reuse Decision**: Determine whether to reuse previous connection parameters
3. **Connection Info Setup**: Get base connection parameters from previous connection or defaults
4. **Connection String Handling**: Parse and merge connection string parameters if provided
5. **Password Management**: Handle password prompting and reuse logic based on parameter changes
6. **Connection Loop**: Attempt connection with retry logic for password authentication
7. **Result Handling**: Update psql state on success or handle failure based on interactive/scripting mode

This function coordinates PostgreSQL connection establishment with sophisticated parameter reuse, authentication, and error handling for both interactive and scripting environments.