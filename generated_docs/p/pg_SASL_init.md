# pg_SASL_init

## Location
[src/interfaces/libpq/fe-auth.c:422-627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L422-L627)

## Overview
Initializes SASL authentication exchange between the PostgreSQL client and server, selecting the appropriate SASL mechanism and preparing the initial authentication response.

## Definition

```c
static int
pg_SASL_init(PGconn *conn, int payloadlen)
```
## Detailed Description
The  function handles the initial phase of SASL (Simple Authentication and Security Layer) authentication in PostgreSQL's libpq client library. It parses the list of SASL authentication mechanisms sent by the server in the AuthenticationSASL message, selects the best supported mechanism based on priority and security requirements, and sends the SASLInitialResponse message back to the server.

The function implements mechanism selection logic that prioritizes SCRAM-SHA-256-PLUS (with channel binding) over SCRAM-SHA-256 when SSL is available and channel binding is not disabled. It validates channel binding requirements and ensures secure authentication practices by preventing downgrade attacks.

## Parameters / Member Variables
- `*conn`: Pointer to the PGconn connection structure containing connection state and configuration
- `payloadlen`: Length of the payload in the AuthenticationSASL message (currently unused in the implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [pqGets](pqGets.md)
  - PQExpBufferDataBroken
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [pqPutMsgStart](pqPutMsgStart.md)
  - [pqPuts](pqPuts.md)
  - [pqPutInt](pqPutInt.md)
  - [pqPutnchar](pqPutnchar.md)
  - [pqPutMsgEnd](pqPutMsgEnd.md)
  - [pqFlush](pqFlush.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
- Constants used:
  - SCRAM_SHA_256_PLUS_NAME
  - SCRAM_SHA_256_NAME
  - PqMsg_SASLInitialResponse
  - STATUS_OK
  - STATUS_ERROR
  - SASL_FAILED
- Called from:
  - [pg_fe_sendauth](pg_fe_sendauth.md)

## Notes and Other Information
- The function prioritizes SCRAM-SHA-256-PLUS over SCRAM-SHA-256 when SSL is available and channel binding is enabled
- Channel binding validation prevents man-in-the-middle attacks by ensuring the authentication is bound to the TLS connection
- The function performs comprehensive error checking for invalid authentication requests, unsupported mechanisms, and missing passwords
- Memory management is handled carefully with proper cleanup in error conditions
- Only supports 'tls-unique' channel binding type currently
- Returns STATUS_OK on success, STATUS_ERROR on failure or out-of-memory conditions

## Simplified Source

```c
static int pg_SASL_init(PGconn *conn, int payloadlen) {
    char *initialresponse = NULL;
    int initialresponselen;
    const char *selected_mechanism;
    PQExpBufferData mechanism_buf;
    char *password = NULL;
    SASLStatus status;

    initPQExpBuffer(&mechanism_buf);

    // Validate channel binding requirements
    if (conn->channel_binding[0] == 'r' && !conn->ssl_in_use) {
        libpq_append_conn_error(conn, "channel binding required, but SSL not in use");
        goto error;
    }

    if (conn->sasl_state) {
        libpq_append_conn_error(conn, "duplicate SASL authentication request");
        goto error;
    }

    // Parse SASL mechanism list from server and select best mechanism
    selected_mechanism = NULL;
    for (;;) {
        if (pqGets(&mechanism_buf, conn))
            goto error;
        if (mechanism_buf.data[0] == '\0')
            break;  // End of mechanism list

        // Prioritize SCRAM-SHA-256-PLUS over SCRAM-SHA-256
        if (strcmp(mechanism_buf.data, SCRAM_SHA_256_PLUS_NAME) == 0) {
            if (conn->ssl_in_use && conn->channel_binding[0] != 'd') {
                selected_mechanism = SCRAM_SHA_256_PLUS_NAME;
                conn->sasl = &pg_scram_mech;
                conn->password_needed = true;
            }
        } else if (strcmp(mechanism_buf.data, SCRAM_SHA_256_NAME) == 0 && !selected_mechanism) {
            selected_mechanism = SCRAM_SHA_256_NAME;
            conn->sasl = &pg_scram_mech;
            conn->password_needed = true;
        }
    }

    if (!selected_mechanism) {
        libpq_append_conn_error(conn, "none of the server's SASL authentication mechanisms are supported");
        goto error;
    }

    // Validate channel binding requirements with selected mechanism
    if (conn->channel_binding[0] == 'r' &&
        strcmp(selected_mechanism, SCRAM_SHA_256_PLUS_NAME) != 0) {
        libpq_append_conn_error(conn, "channel binding is required, but server did not offer an authentication method that supports channel binding");
        goto error;
    }

    // Get password for authentication
    if (conn->password_needed) {
        password = conn->connhost[conn->whichhost].password;
        if (password == NULL)
            password = conn->pgpass;
        if (password == NULL || password[0] == '\0') {
            appendPQExpBufferStr(&conn->errorMessage, PQnoPasswordSupplied);
            goto error;
        }
    }

    // Initialize SASL state and get initial response
    conn->sasl_state = conn->sasl->init(conn, password, selected_mechanism);
    if (!conn->sasl_state)
        goto oom_error;

    status = conn->sasl->exchange(conn->sasl_state, NULL, -1,
                                  &initialresponse, &initialresponselen);
    if (status == SASL_FAILED)
        goto error;

    // Send SASLInitialResponse message to server
    if (pqPutMsgStart(PqMsg_SASLInitialResponse, conn) ||
        pqPuts(selected_mechanism, conn) ||
        (initialresponse && (pqPutInt(initialresponselen, 4, conn) ||
                             pqPutnchar(initialresponse, initialresponselen, conn))) ||
        pqPutMsgEnd(conn) ||
        pqFlush(conn))
        goto error;

    termPQExpBuffer(&mechanism_buf);
    free(initialresponse);
    return STATUS_OK;

error:
    termPQExpBuffer(&mechanism_buf);
    free(initialresponse);
    return STATUS_ERROR;

oom_error:
    termPQExpBuffer(&mechanism_buf);
    free(initialresponse);
    libpq_append_conn_error(conn, "out of memory");
    return STATUS_ERROR;
}
```