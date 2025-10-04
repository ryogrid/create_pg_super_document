# pqsecure_open_gss

## Location
[src/interfaces/libpq/fe-secure-gssapi.c:479-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-gssapi.c#L479-L755)

## Overview
Negotiates GSSAPI transport encryption for a PostgreSQL connection, managing the complete handshake process and buffer setup for secure communication.

## Definition

```c
PostgresPollingStatusType
pqsecure_open_gss(PGconn *conn)
```
## Detailed Description
This function implements the complete GSSAPI transport negotiation process for PostgreSQL connections. It manages a complex state machine that handles multiple phases: initial buffer allocation, credential delegation setup, GSS context initialization, packet exchange with the server, and final buffer resizing for normal operation. The function operates in a non-blocking manner, returning polling status codes to indicate when the caller should retry based on socket readiness.

Key phases:
1. **Initialization**: Allocates authentication-sized buffers (PQ_GSS_AUTH_BUFFER_SIZE)
2. **Token Exchange**: Sends/receives GSSAPI tokens with the server using gss_init_sec_context
3. **Error Handling**: Processes server error packets during negotiation  
4. **Completion**: Resizes buffers to normal operation size (PQ_GSS_MAX_PACKET_SIZE) and determines maximum packet size

The function maintains state across multiple calls, handling partial reads/writes and resuming from the correct point in the negotiation process.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection object containing GSSAPI context, credentials, buffers, and socket information
## Dependencies
- Functions called/Symbols referenced:
  - gss_init_sec_context (primary GSSAPI negotiation function)
  - gss_wrap_size_limit (determines maximum packet size for encryption)
  - gss_release_buffer, gss_release_cred (GSSAPI resource cleanup)
  - [pg_GSS_load_servicename](pg_GSS_load_servicename.md) (loads Kerberos service principal)
  - [pg_GSS_have_cred_cache](pg_GSS_have_cred_cache.md) (acquires cached credentials for delegation)
  - [pg_GSS_error](pg_GSS_error.md) (error reporting)
  - [gss_read](../g/gss_read.md) (internal wrapper for receiving data)
  - [pqsecure_raw_write](pqsecure_raw_write.md) (low-level socket write)
  - pg_hton32, pg_ntoh32 (network byte order conversion)
- Called from:
  - CONNECTION_FAILED state handler in fe-connect.c during connection establishment

## Notes and Other Information
- Uses different buffer sizes during negotiation (PQ_GSS_AUTH_BUFFER_SIZE) vs normal operation (PQ_GSS_MAX_PACKET_SIZE)
- Handles credential delegation when gssdelegation connection parameter is enabled
- Implements special error packet detection (packets starting with 'E') during startup phase
- Sets conn->gssenc and conn->gssapi_used flags upon successful completion
- Returns PGRES_POLLING_OK (success), PGRES_POLLING_READING/WRITING (retry needed), or PGRES_POLLING_FAILED (error)
- Critical for establishing secure GSSAPI transport before any application data exchange
- Manages global state variables for send/receive buffers that are later used by pg_GSS_read/write functions

## Simplified Source
```c
PostgresPollingStatusType pqsecure_open_gss(PGconn *conn) {
    ssize_t ret;
    OM_uint32 major, minor, gss_flags = GSS_REQUIRED_FLAGS;
    uint32 netlen;
    PostgresPollingStatusType result;
    gss_buffer_desc input = GSS_C_EMPTY_BUFFER, output = GSS_C_EMPTY_BUFFER;

    // Initialize buffers on first call
    if (PqGSSSendBuffer == NULL) {
        PqGSSSendBuffer = malloc(PQ_GSS_AUTH_BUFFER_SIZE);
        PqGSSRecvBuffer = malloc(PQ_GSS_AUTH_BUFFER_SIZE);
        PqGSSResultBuffer = malloc(PQ_GSS_AUTH_BUFFER_SIZE);
        if (!PqGSSSendBuffer || !PqGSSRecvBuffer || !PqGSSResultBuffer) {
            libpq_append_conn_error(conn, "out of memory");
            return PGRES_POLLING_FAILED;
        }
        PqGSSSendLength = PqGSSSendNext = PqGSSSendConsumed = 0;
        PqGSSRecvLength = PqGSSResultLength = PqGSSResultNext = 0;
    }

    // Send any pending data from previous call
    if (PqGSSSendLength) {
        ssize_t amount = PqGSSSendLength - PqGSSSendNext;
        ret = pqsecure_raw_write(conn, PqGSSSendBuffer + PqGSSSendNext, amount);
        if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                return PGRES_POLLING_WRITING;
            else
                return PGRES_POLLING_FAILED;
        }
        if (ret < amount) {
            PqGSSSendNext += ret;
            return PGRES_POLLING_WRITING;
        }
        PqGSSSendLength = PqGSSSendNext = 0;
    }

    // Process incoming data if context exists
    if (conn->gctx) {
        // Read packet length if needed
        if (PqGSSRecvLength < sizeof(uint32)) {
            result = gss_read(conn, PqGSSRecvBuffer + PqGSSRecvLength,
                             sizeof(uint32) - PqGSSRecvLength, &ret);
            if (result != PGRES_POLLING_OK)
                return result;
            PqGSSRecvLength += ret;
            if (PqGSSRecvLength < sizeof(uint32))
                return PGRES_POLLING_READING;
        }

        // Handle error packets
        if (PqGSSRecvBuffer[0] == 'E') {
            result = gss_read(conn, PqGSSRecvBuffer + PqGSSRecvLength,
                             PQ_GSS_AUTH_BUFFER_SIZE - PqGSSRecvLength - 1, &ret);
            if (result != PGRES_POLLING_OK)
                return result;
            PqGSSRecvLength += ret;
            PqGSSRecvBuffer[PqGSSRecvLength] = '\0';
            appendPQExpBuffer(&conn->errorMessage, "%s\n", PqGSSRecvBuffer + 1);
            return PGRES_POLLING_FAILED;
        }

        // Read rest of packet
        input.length = pg_ntoh32(*(uint32 *) PqGSSRecvBuffer);
        if (input.length > PQ_GSS_AUTH_BUFFER_SIZE - sizeof(uint32)) {
            libpq_append_conn_error(conn, "oversize GSSAPI packet sent by the server");
            return PGRES_POLLING_FAILED;
        }

        result = gss_read(conn, PqGSSRecvBuffer + PqGSSRecvLength,
                         input.length - (PqGSSRecvLength - sizeof(uint32)), &ret);
        if (result != PGRES_POLLING_OK)
            return result;

        PqGSSRecvLength += ret;
        if (PqGSSRecvLength - sizeof(uint32) < input.length)
            return PGRES_POLLING_READING;

        input.value = PqGSSRecvBuffer + sizeof(uint32);
    }

    // Load service name and setup credentials
    ret = pg_GSS_load_servicename(conn);
    if (ret != STATUS_OK)
        return PGRES_POLLING_FAILED;

    if (conn->gssdelegation && conn->gssdelegation[0] == '1') {
        if (conn->gcred == GSS_C_NO_CREDENTIAL)
            (void) pg_GSS_have_cred_cache(&conn->gcred);
        if (conn->gcred != GSS_C_NO_CREDENTIAL)
            gss_flags |= GSS_C_DELEG_FLAG;
    }

    // Call GSS init context
    major = gss_init_sec_context(&minor, conn->gcred, &conn->gctx,
                                conn->gtarg_nam, GSS_C_NO_OID,
                                gss_flags, 0, 0, &input, NULL,
                                &output, NULL, NULL);

    PqGSSRecvLength = 0;

    if (GSS_ERROR(major)) {
        pg_GSS_error(libpq_gettext("could not initiate GSSAPI security context"),
                    conn, major, minor);
        return PGRES_POLLING_FAILED;
    }

    if (output.length == 0) {
        // Negotiation complete - setup for normal operation
        conn->gssenc = true;
        conn->gssapi_used = true;

        // Cleanup and resize buffers
        gss_release_cred(&minor, &conn->gcred);
        conn->gcred = GSS_C_NO_CREDENTIAL;
        gss_release_buffer(&minor, &output);

        free(PqGSSSendBuffer);
        free(PqGSSRecvBuffer);
        free(PqGSSResultBuffer);
        PqGSSSendBuffer = malloc(PQ_GSS_MAX_PACKET_SIZE);
        PqGSSRecvBuffer = malloc(PQ_GSS_MAX_PACKET_SIZE);
        PqGSSResultBuffer = malloc(PQ_GSS_MAX_PACKET_SIZE);
        if (!PqGSSSendBuffer || !PqGSSRecvBuffer || !PqGSSResultBuffer) {
            libpq_append_conn_error(conn, "out of memory");
            return PGRES_POLLING_FAILED;
        }
        PqGSSSendLength = PqGSSSendNext = PqGSSSendConsumed = 0;
        PqGSSRecvLength = PqGSSResultLength = PqGSSResultNext = 0;

        // Determine max packet size
        major = gss_wrap_size_limit(&minor, conn->gctx, 1, GSS_C_QOP_DEFAULT,
                                   PQ_GSS_MAX_PACKET_SIZE - sizeof(uint32),
                                   &PqGSSMaxPktSize);
        if (GSS_ERROR(major)) {
            pg_GSS_error(libpq_gettext("GSSAPI size check error"), conn, major, minor);
            return PGRES_POLLING_FAILED;
        }

        return PGRES_POLLING_OK;
    }

    // Queue output token for sending
    if (output.length > PQ_GSS_AUTH_BUFFER_SIZE - sizeof(uint32)) {
        libpq_append_conn_error(conn, "client tried to send oversize GSSAPI packet");
        gss_release_buffer(&minor, &output);
        return PGRES_POLLING_FAILED;
    }

    netlen = pg_hton32(output.length);
    memcpy(PqGSSSendBuffer, (char *) &netlen, sizeof(uint32));
    PqGSSSendLength = sizeof(uint32);
    memcpy(PqGSSSendBuffer + PqGSSSendLength, output.value, output.length);
    PqGSSSendLength += output.length;

    gss_release_buffer(&minor, &output);
    return PGRES_POLLING_WRITING;
}
```