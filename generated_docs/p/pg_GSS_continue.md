# pg_GSS_continue

## Location
[src/interfaces/libpq/fe-auth.c:58-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L58-L160)

## Overview
Continues GSS authentication with the next token in a multi-step GSSAPI authentication handshake between the PostgreSQL client and server.

## Definition

```c
static int
pg_GSS_continue(PGconn *conn, int payloadlen)
```
## Detailed Description
This function handles the continuation of GSSAPI authentication after the initial startup. It manages the exchange of authentication tokens between the client and server through multiple round trips. The function performs the following key operations:

1. **Token Processing**: Reads incoming authentication tokens from the server (if any) into a GSS buffer
2. **Credential Management**: Checks for credential cache availability and handles credential delegation if enabled
3. **Security Context**: Calls  to advance the authentication state machine
4. **Response Generation**: Sends any generated authentication data back to the server
5. **State Management**: Tracks authentication completion and manages GSS context lifecycle

The function supports both initial calls (no input token) and subsequent calls (with server response tokens) during the multi-step authentication process.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection structure containing GSS context, credentials, and connection state
- `payloadlen`: Length of the incoming authentication token from the server (0 for initial call)
## Dependencies
- Functions called/Symbols referenced:
  -  - Memory allocation for input token buffer
  -  - Reads authentication token from connection buffer
  -  - Checks for available credential cache
  -  - Core GSSAPI function to advance authentication
  -  - Sends response token to server
  -  - Error reporting for GSS failures
  -  - Memory cleanup for GSS buffers
  -  - Cleanup for GSS name objects
  -  - Cleanup for GSS security context
- Called from (representative examples):
  -  - Initial GSS authentication setup
  -  - Main authentication dispatcher

## Notes and Other Information
- This is a static function internal to the libpq authentication module
- Handles memory management carefully with proper cleanup on error paths
- Supports credential delegation when enabled via connection parameters
- Sets  and  flags upon successful completion
- Uses mutual authentication flag (GSS_C_MUTUAL_FLAG) for enhanced security
- Returns STATUS_OK on success, STATUS_ERROR on failure

## Simplified Source

```c
static int pg_GSS_continue(PGconn *conn, int payloadlen) {
    OM_uint32 maj_stat, min_stat, lmin_s, gss_flags = GSS_C_MUTUAL_FLAG;
    gss_buffer_desc ginbuf;
    gss_buffer_desc goutbuf;

    // Read input token from server (if continuing authentication)
    if (conn->gctx != GSS_C_NO_CONTEXT) {
        ginbuf.length = payloadlen;
        ginbuf.value = malloc(payloadlen);
        if (!ginbuf.value) {
            libpq_append_conn_error(conn, "out of memory allocating GSSAPI buffer (%d)", payloadlen);
            return STATUS_ERROR;
        }
        if (pqGetnchar(ginbuf.value, payloadlen, conn)) {
            free(ginbuf.value);
            return STATUS_ERROR;
        }
    } else {
        ginbuf.length = 0;
        ginbuf.value = NULL;
    }

    // Check credentials and set delegation flag if enabled
    if (!pg_GSS_have_cred_cache(&conn->gcred))
        conn->gcred = GSS_C_NO_CREDENTIAL;
    if (conn->gssdelegation && conn->gssdelegation[0] == '1')
        gss_flags |= GSS_C_DELEG_FLAG;

    // Perform GSS authentication step
    maj_stat = gss_init_sec_context(&min_stat, conn->gcred, &conn->gctx,
                                    conn->gtarg_nam, GSS_C_NO_OID, gss_flags,
                                    0, GSS_C_NO_CHANNEL_BINDINGS,
                                    (ginbuf.value == NULL) ? GSS_C_NO_BUFFER : &ginbuf,
                                    NULL, &goutbuf, NULL, NULL);

    free(ginbuf.value);

    // Send response token to server if generated
    if (goutbuf.length != 0) {
        if (pqPacketSend(conn, PqMsg_GSSResponse, goutbuf.value, goutbuf.length) != STATUS_OK) {
            gss_release_buffer(&lmin_s, &goutbuf);
            return STATUS_ERROR;
        }
    }
    gss_release_buffer(&lmin_s, &goutbuf);

    // Handle authentication completion or errors
    if (maj_stat != GSS_S_COMPLETE && maj_stat != GSS_S_CONTINUE_NEEDED) {
        pg_GSS_error(libpq_gettext("GSSAPI continuation error"), conn, maj_stat, min_stat);
        // Cleanup on error
        gss_release_name(&lmin_s, &conn->gtarg_nam);
        if (conn->gctx)
            gss_delete_sec_context(&lmin_s, &conn->gctx, GSS_C_NO_BUFFER);
        return STATUS_ERROR;
    }

    if (maj_stat == GSS_S_COMPLETE) {
        conn->client_finished_auth = true;
        gss_release_name(&lmin_s, &conn->gtarg_nam);
        conn->gssapi_used = true;
    }

    return STATUS_OK;
}
```