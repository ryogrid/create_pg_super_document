# pqConnectOptions2

## Location
[src/interfaces/libpq/fe-connect.c:1120-1880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L1120-L1880)

## Overview
Computes derived connection options after absorbing all user-supplied information and validates them for PostgreSQL connections.

## Definition

```c
bool pqConnectOptions2(PGconn *conn)
```
## Detailed Description
This function performs comprehensive validation and processing of connection parameters for a PostgreSQL connection. It takes the raw connection parameters provided by the user and transforms them into a structured format suitable for establishing connections. The function handles multiple hosts, validates SSL/TLS options, processes authentication requirements, and sets up connection-specific configurations.

Key responsibilities include:
- Parsing and validating host/hostaddr/port combinations for multi-host connections
- Setting up host connection structures (pg_conn_host) for each potential target
- Validating and processing SSL/TLS configuration options
- Processing authentication method requirements (require_auth parameter)
- Handling password file lookups when passwords are not explicitly provided
- Validating GSSAPI encryption settings
- Processing target session attributes and load balancing options
- Resolving client encoding from locale when set to "auto"

The function returns true on success and false on failure, setting appropriate error messages and connection status.

## Parameters / Member Variables
- `conn`: Pointer to PGconn structure containing connection parameters to be processed and validated

## Dependencies
- Functions called/Symbols referenced:
  - [count_comma_separated_elems](../c/count_comma_separated_elems.md)
  - [parse_comma_separated_list](parse_comma_separated_list.md)
  - [pg_fe_getauthname](pg_fe_getauthname.md)
  - [passwordFromFile](passwordFromFile.md)
  - [pqGetHomeDirectory](pqGetHomeDirectory.md)
  - [sslVerifyProtocolVersion](../s/sslVerifyProtocolVersion.md)
  - [sslVerifyProtocolRange](../s/sslVerifyProtocolRange.md)
  - [libpq_prng_init](../l/libpq_prng_init.md)
  - [pg_prng_uint64_range](pg_prng_uint64_range.md)
  - [pg_encoding_to_char](pg_encoding_to_char.md)
  - [pg_get_encoding_from_locale](pg_get_encoding_from_locale.md)
- Called from (representative examples):
  - [PQconnectStartParams](../P/PQconnectStartParams.md)
  - [PQconnectStart](../P/PQconnectStart.md)  
  - [PQsetdbLogin](../P/PQsetdbLogin.md)
  - [PQcancelCreate](../P/PQcancelCreate.md)

## Notes and Other Information
- The function allocates memory for pg_conn_host structures based on the number of hosts specified
- It implements Fisher-Yates shuffle algorithm for random load balancing of hosts
- SSL/TLS validation is conditional on compile-time SSL support
- The function sets conn->options_valid = true only upon successful completion
- Error handling includes specific out-of-memory error paths
- Host type detection distinguishes between Unix sockets, host names, and IP addresses
- Password file lookup follows the standard ~/.pgpass format and precedence rules

## Simplified Source

```c
bool pqConnectOptions2(PGconn *conn)
{
    int i;

    // Phase 1: Setup host connection structures
    conn->whichhost = 0;

    // Count number of hosts from hostaddr or host parameters
    if (conn->pghostaddr && conn->pghostaddr[0] != '\0')
        conn->nconnhost = count_comma_separated_elems(conn->pghostaddr);
    else if (conn->pghost && conn->pghost[0] != '\0')
        conn->nconnhost = count_comma_separated_elems(conn->pghost);
    else
        conn->nconnhost = 1;

    // Allocate host connection structures
    conn->connhost = (pg_conn_host *) calloc(conn->nconnhost, sizeof(pg_conn_host));
    if (conn->connhost == NULL)
        goto oom_error;

    // Parse hostaddr values
    if (conn->pghostaddr && conn->pghostaddr[0] != '\0') {
        parse_host_addresses(conn->pghostaddr, conn->connhost, conn->nconnhost);
    }

    // Parse host values
    if (conn->pghost && conn->pghost[0] != '\0') {
        parse_host_names(conn->pghost, conn->connhost, conn->nconnhost);
    }

    // Phase 2: Determine host types (Unix socket, hostname, or IP address)
    for (i = 0; i < conn->nconnhost; i++) {
        pg_conn_host *ch = &conn->connhost[i];

        if (ch->hostaddr && ch->hostaddr[0] != '\0')
            ch->type = CHT_HOST_ADDRESS;
        else if (ch->host && ch->host[0] != '\0') {
            ch->type = is_unixsock_path(ch->host) ? CHT_UNIX_SOCKET : CHT_HOST_NAME;
        } else {
            // Set default host
            ch->host = strdup(DEFAULT_PGSOCKET_DIR[0] ? DEFAULT_PGSOCKET_DIR : DefaultHost);
            ch->type = DEFAULT_PGSOCKET_DIR[0] ? CHT_UNIX_SOCKET : CHT_HOST_NAME;
        }
    }

    // Phase 3: Parse port numbers
    if (conn->pgport && conn->pgport[0] != '\0') {
        parse_port_numbers(conn->pgport, conn->connhost, conn->nconnhost);
    }

    // Phase 4: Set default user and database
    if (!conn->pguser || conn->pguser[0] == '\0') {
        free(conn->pguser);
        conn->pguser = pg_fe_getauthname(&conn->errorMessage);
        if (!conn->pguser) {
            conn->status = CONNECTION_BAD;
            return false;
        }
    }

    if (!conn->dbName || conn->dbName[0] == '\0') {
        free(conn->dbName);
        conn->dbName = strdup(conn->pguser);
        if (!conn->dbName)
            goto oom_error;
    }

    // Phase 5: Password file lookup
    if (!conn->pgpass || conn->pgpass[0] == '\0') {
        setup_password_file_path(conn);
        lookup_passwords_from_file(conn);
    }

    // Phase 6: Validate authentication requirements
    if (conn->require_auth && conn->require_auth[0]) {
        if (!parse_and_validate_auth_methods(conn))
            return false;
    }

    // Phase 7: Validate SSL/TLS options
    if (!validate_ssl_options(conn))
        return false;

    // Phase 8: Validate GSSAPI options
    if (!validate_gss_options(conn))
        return false;

    // Phase 9: Set target server type and load balancing
    set_target_server_type(conn);
    setup_load_balancing(conn);

    // Phase 10: Resolve client encoding
    if (conn->client_encoding_initial &&
        strcmp(conn->client_encoding_initial, "auto") == 0) {
        free(conn->client_encoding_initial);
        conn->client_encoding_initial = strdup(pg_encoding_to_char(
            pg_get_encoding_from_locale(NULL, true)));
        if (!conn->client_encoding_initial)
            goto oom_error;
    }

    // Mark options as valid
    conn->options_valid = true;
    return true;

oom_error:
    conn->status = CONNECTION_BAD;
    libpq_append_conn_error(conn, "out of memory");
    return false;
}
```