# PQconnectPoll

## Location
[src/interfaces/libpq/fe-connect.c:2596-2873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L2596-L2873)

## Overview
`PQconnectPoll` is a public libpq function that performs non-blocking polling of an asynchronous PostgreSQL database connection, advancing the connection state machine without blocking program execution.

## Definition
```c
PostgresPollingStatusType PQconnectPoll(PGconn *conn)
```

## Detailed Description
This function implements the core non-blocking connection establishment mechanism for PostgreSQL. It advances the connection through various states including hostname resolution, socket connection, SSL/GSS negotiation, authentication, and final connection establishment. The function handles multiple hosts/addresses with load balancing support (including random shuffling), connection timeouts, and automatic failover between servers. It manages the connection state machine through various states like CONNECTION_STARTED, CONNECTION_MADE, CONNECTION_AUTH_OK, etc., and handles both reading and writing states appropriately. The function supports Unix domain sockets, IPv4/IPv6 addresses, and hostname resolution, with special handling for prefer-standby connection modes.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure representing the database connection being polled

## Dependencies
- Functions called/Symbols referenced:
  - [pqReadData](../p/pqReadData.md): Reads incoming data from the connection
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md): Appends error messages to connection
  - [release_conn_addrinfo](../r/release_conn_addrinfo.md): Releases address information for previous host
  - [pqParseIntParam](../p/pqParseIntParam.md): Parses integer parameters like port numbers
  - `[pg_getaddrinfo_all](../p/pg_getaddrinfo_all.md)`: Resolves hostnames to network addresses
  - [store_conn_addrinfo](../s/store_conn_addrinfo.md): Stores address information in connection structure
  - `[pg_freeaddrinfo_all](../p/pg_freeaddrinfo_all.md)`: Frees address information
  - `[pg_prng_uint64_range](../p/pg_prng_uint64_range.md)`: Generates random numbers for load balancing
  - [pqDropConnection](../p/pqDropConnection.md): Closes existing connection
  - [pqDropServerData](../p/pqDropServerData.md): Clears server-specific data
  - [pqClearAsyncResult](../p/pqClearAsyncResult.md): Clears asynchronous result data
  - Connection state constants: CONNECTION_BAD, CONNECTION_OK, CONNECTION_NEEDED, etc.
  - Polling status constants: PGRES_POLLING_OK, PGRES_POLLING_READING, PGRES_POLLING_WRITING, PGRES_POLLING_FAILED

- Called from (representative examples):
  - [pqConnectDBComplete](../p/pqConnectDBComplete.md): Blocking connection completion
  - [PQresetPoll](PQresetPoll.md): Connection reset polling
  - [pqConnectDBStart](../p/pqConnectDBStart.md): Connection startup
  - [PQcancelPoll](PQcancelPoll.md): Connection cancellation polling
  - [libpqrcv_connect](../l/libpqrcv_connect.md): Replication connection establishment
  - [wait_until_connected](../w/wait_until_connected.md): psql connection waiting
  - [libpqsrv_connect_internal](../l/libpqsrv_connect_internal.md): Server-side connection helper

## Notes and Other Information
This is a public API function that applications can call directly to implement non-blocking connection establishment. It should be used in conjunction with `PQconnectStart()` and requires the application to use `select()` or similar mechanisms to determine when to call it based on socket readiness. The function returns different PostgresPollingStatusType values: PGRES_POLLING_OK (connection complete), PGRES_POLLING_READING (waiting for socket readable), PGRES_POLLING_WRITING (waiting for socket writable), or PGRES_POLLING_FAILED (connection failed). Applications must call `PQfinish()` regardless of success or failure. The function includes important caveats about blocking behavior with hostname resolution, Kerberos authentication, and tracing operations.

## Simplified Source

```c
PostgresPollingStatusType PQconnectPoll(PGconn *conn)
{
    if (conn == NULL)
        return PGRES_POLLING_FAILED;

    // Handle already established states
    switch (conn->status) {
        case CONNECTION_BAD:
            return PGRES_POLLING_FAILED;
        case CONNECTION_OK:
            return PGRES_POLLING_OK;

        // Reading states - wait for data
        case CONNECTION_AWAITING_RESPONSE:
        case CONNECTION_AUTH_OK:
        case CONNECTION_CHECK_WRITABLE:
        case CONNECTION_CONSUME:
        case CONNECTION_CHECK_STANDBY:
            if (pqReadData(conn) < 0)
                goto error_return;
            if (pqReadData(conn) == 0)
                return PGRES_POLLING_READING;
            break;

        // Writing states - continue processing
        case CONNECTION_STARTED:
        case CONNECTION_MADE:
        case CONNECTION_SSL_STARTUP:
        case CONNECTION_NEEDED:
        case CONNECTION_GSS_STARTUP:
        case CONNECTION_CHECK_TARGET:
            break;

        default:
            libpq_append_conn_error(conn, "invalid connection state");
            goto error_return;
    }

keep_going:
    // Try next address if current one failed
    if (conn->try_next_addr) {
        if (conn->whichaddr < conn->naddr) {
            conn->whichaddr++;
            reset_connection_state_machine = true;
        } else {
            conn->try_next_host = true;
        }
        conn->try_next_addr = false;
    }

    // Try next host if no more addresses
    if (conn->try_next_host) {
        if (conn->whichhost + 1 < conn->nconnhost) {
            conn->whichhost++;
        } else {
            // Handle prefer-standby mode fallback
            if (conn->target_server_type == SERVER_TYPE_PREFER_STANDBY &&
                conn->nconnhost > 0 && !conn->cancelRequest) {
                conn->target_server_type = SERVER_TYPE_PREFER_STANDBY_PASS2;
                conn->whichhost = 0;
            } else {
                goto error_return;
            }
        }

        // Resolve new host address
        pg_conn_host *ch = &conn->connhost[conn->whichhost];
        struct addrinfo *addrlist;

        // Get port number
        int thisport = (ch->port && ch->port[0]) ?
                       parse_port(ch->port) : DEF_PGPORT;

        // Resolve address based on connection type
        switch (ch->type) {
            case CHT_HOST_NAME:
                resolve_hostname(ch->host, thisport, &addrlist);
                break;
            case CHT_HOST_ADDRESS:
                resolve_hostaddr(ch->hostaddr, thisport, &addrlist);
                break;
            case CHT_UNIX_SOCKET:
                resolve_unix_socket(ch->host, thisport, &addrlist);
                break;
        }

        // Store address info and optionally shuffle for load balancing
        store_conn_addrinfo(conn, addrlist);
        if (conn->load_balance_type == LOAD_BALANCE_RANDOM) {
            shuffle_addresses(conn);
        }

        reset_connection_state_machine = true;
        conn->try_next_host = false;
    }

    // Reset connection state for new server
    if (reset_connection_state_machine) {
        conn->pversion = PG_PROTOCOL(3, 0);
        conn->send_appname = true;
        conn->failed_enc_methods = 0;
        need_new_connection = true;
    }

    // Start new connection attempt
    if (need_new_connection) {
        pqDropConnection(conn, true);
        pqDropServerData(conn);
        conn->asyncStatus = PGASYNC_IDLE;
        conn->status = CONNECTION_NEEDED;
        need_new_connection = false;
    }

    // Continue with connection state machine processing
    return advance_connection_state_machine(conn);

error_return:
    conn->status = CONNECTION_BAD;
    return PGRES_POLLING_FAILED;
}
```