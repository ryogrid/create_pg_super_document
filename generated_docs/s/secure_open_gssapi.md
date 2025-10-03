# secure_open_gssapi

## Location
[src/backend/libpq/be-secure-gssapi.c:502-740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-gssapi.c#L502-L740)

## Overview
Establishes a GSSAPI-encrypted connection by performing the complete GSSAPI authentication handshake with the client.

## Definition

```c
ssize_t
secure_open_gssapi(Port *port)
```
## Detailed Description
The  function performs the complete GSSAPI authentication and encryption setup process for a PostgreSQL backend connection. This is a blocking function that handles the multi-round handshake protocol required to establish a secure GSSAPI session.

The function operates in several phases:
1. **Initialization**: Allocates GSSAPI state structures and communication buffers
2. **Keytab Setup**: Configures Kerberos keytab file if specified
3. **Authentication Loop**: Exchanges authentication tokens with the client using 
4. **Buffer Management**: Handles reading complete packets from client and sending responses
5. **Finalization**: Resizes buffers for normal operation and determines maximum packet size

The function uses smaller buffers during authentication () then switches to larger buffers () for normal encrypted communication. It handles credential delegation if configured and manages all GSSAPI error conditions.

Unlike the streaming read/write functions, this function will block on socket operations using  to ensure the handshake completes properly.

## Parameters / Member Variables
- `*port`: Pointer to Port structure that will be configured with GSSAPI encryption state
## Dependencies
- Functions called/Symbols referenced:
  - : Blocking read helper for complete packet reception
  - : GSSAPI function to process authentication tokens
  - : Determines maximum packet size for encryption
  - : Releases GSSAPI-allocated buffers
  - : PostgreSQL GSSAPI error reporting
  - : Stores delegated Kerberos credentials
  - : Low-level socket write function
  - : Wait for socket readiness
  - : PostgreSQL memory allocation
  - : Sets KRB5_KTNAME environment variable for keytab
- Global buffers managed:
  - , , : Communication buffers
  - , , etc.: Buffer state variables
  - : Maximum packet size for encryption
- Configuration variables:
  - : Path to Kerberos keytab file
  - : Whether to accept delegated credentials
- Called from:
  - : During connection establishment when GSSAPI is negotiated

## Notes and Other Information
- Returns 0 on success, -1 on failure with appropriate error logging
- Sets  to true when encryption is successfully established  
- Allocates  structure in  for connection lifetime
- Handles both regular authentication and credential delegation scenarios
- Enforces packet size limits to prevent memory exhaustion attacks
- The authentication buffer size is smaller than normal operation buffers for efficiency
- Uses network byte order for packet length headers in the protocol
- Function will block until authentication completes or fails, unlike the non-blocking read/write functions
- Supports cleanup of partial state on errors through proper buffer deallocation

## Simplified Source

```c
// Simplified version of secure_open_gssapi
ssize_t secure_open_gssapi(Port *port) {
    bool auth_complete = false;
    OM_uint32 major, minor;
    gss_cred_id_t delegated_creds = GSS_C_NO_CREDENTIAL;

    // Initialize GSSAPI structures and buffers
    port->gss = MemoryContextAllocZero(TopMemoryContext, sizeof(pg_gssinfo));
    port->gss->delegated_creds = false;

    // Allocate authentication buffers (smaller size during handshake)
    PqGSSSendBuffer = malloc(PQ_GSS_AUTH_BUFFER_SIZE);
    PqGSSRecvBuffer = malloc(PQ_GSS_AUTH_BUFFER_SIZE);
    PqGSSResultBuffer = malloc(PQ_GSS_AUTH_BUFFER_SIZE);
    if (!PqGSSSendBuffer || !PqGSSRecvBuffer || !PqGSSResultBuffer)
        ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    // Initialize buffer state variables
    PqGSSSendLength = PqGSSSendNext = PqGSSSendConsumed = 0;
    PqGSSRecvLength = PqGSSResultLength = PqGSSResultNext = 0;

    // Configure Kerberos keytab if specified
    if (pg_krb_server_keyfile && pg_krb_server_keyfile[0] != '\0') {
        if (setenv("KRB5_KTNAME", pg_krb_server_keyfile, 1) != 0)
            ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY),
                           errmsg("could not set environment: %m")));
    }

    // Main authentication loop - exchange tokens with client
    while (!auth_complete) {
        gss_buffer_desc input, output = GSS_C_EMPTY_BUFFER;

        // Read packet length header from client
        if (read_or_wait(port, sizeof(uint32)) < 0)
            return -1;

        // Extract packet length and validate size
        input.length = pg_ntoh32(*(uint32 *) PqGSSRecvBuffer);
        PqGSSRecvLength = 0;

        if (input.length > PQ_GSS_AUTH_BUFFER_SIZE - sizeof(uint32)) {
            ereport(COMMERROR, (errmsg("oversize GSSAPI packet from client")));
            return -1;
        }

        // Read the actual authentication data
        if (read_or_wait(port, input.length) < 0)
            return -1;
        input.value = PqGSSRecvBuffer;

        // Process the GSSAPI authentication token
        major = gss_accept_sec_context(&minor, &port->gss->ctx,
                                       GSS_C_NO_CREDENTIAL, &input,
                                       GSS_C_NO_CHANNEL_BINDINGS,
                                       &port->gss->name, NULL, &output, NULL,
                                       NULL, pg_gss_accept_delegation ? &delegated_creds : NULL);

        if (GSS_ERROR(major)) {
            pg_GSS_error("could not accept GSSAPI security context", major, minor);
            gss_release_buffer(&minor, &output);
            return -1;
        }

        // Check if authentication is complete
        if (!(major & GSS_S_CONTINUE_NEEDED))
            auth_complete = true;

        // Handle credential delegation if provided
        if (delegated_creds != GSS_C_NO_CREDENTIAL) {
            pg_store_delegated_credential(delegated_creds);
            port->gss->delegated_creds = true;
        }

        PqGSSRecvLength = 0;

        // Send response token to client if we have one
        if (output.length > 0) {
            uint32 net_length = pg_hton32(output.length);

            if (output.length > PQ_GSS_AUTH_BUFFER_SIZE - sizeof(uint32)) {
                ereport(COMMERROR, (errmsg("server GSSAPI packet too large")));
                gss_release_buffer(&minor, &output);
                return -1;
            }

            // Build packet: length header + data
            memcpy(PqGSSSendBuffer, &net_length, sizeof(uint32));
            memcpy(PqGSSSendBuffer + sizeof(uint32), output.value, output.length);
            PqGSSSendLength = sizeof(uint32) + output.length;
            PqGSSSendNext = 0;

            // Send complete packet to client with retry logic
            while (PqGSSSendNext < PqGSSSendLength) {
                ssize_t bytes_sent = secure_raw_write(port,
                                                     PqGSSSendBuffer + PqGSSSendNext,
                                                     PqGSSSendLength - PqGSSSendNext);

                if (bytes_sent < 0 && errno != EWOULDBLOCK && errno != EAGAIN && errno != EINTR) {
                    gss_release_buffer(&minor, &output);
                    return -1;
                }

                if (bytes_sent <= 0) {
                    // Wait for socket to become writable
                    WaitLatchOrSocket(MyLatch, WL_SOCKET_WRITEABLE | WL_EXIT_ON_PM_DEATH,
                                     port->sock, 0, WAIT_EVENT_GSS_OPEN_SERVER);
                    continue;
                }

                PqGSSSendNext += bytes_sent;
            }

            PqGSSSendLength = PqGSSSendNext = 0;
            gss_release_buffer(&minor, &output);
        }
    }

    // Switch to larger buffers for normal encrypted operation
    free(PqGSSSendBuffer);
    free(PqGSSRecvBuffer);
    free(PqGSSResultBuffer);

    PqGSSSendBuffer = malloc(PQ_GSS_MAX_PACKET_SIZE);
    PqGSSRecvBuffer = malloc(PQ_GSS_MAX_PACKET_SIZE);
    PqGSSResultBuffer = malloc(PQ_GSS_MAX_PACKET_SIZE);
    if (!PqGSSSendBuffer || !PqGSSRecvBuffer || !PqGSSResultBuffer)
        ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    // Reset buffer state for normal operation
    PqGSSSendLength = PqGSSSendNext = PqGSSSendConsumed = 0;
    PqGSSRecvLength = PqGSSResultLength = PqGSSResultNext = 0;

    // Determine maximum packet size for encryption
    major = gss_wrap_size_limit(&minor, port->gss->ctx, 1, GSS_C_QOP_DEFAULT,
                               PQ_GSS_MAX_PACKET_SIZE - sizeof(uint32),
                               &PqGSSMaxPktSize);

    if (GSS_ERROR(major)) {
        pg_GSS_error("GSSAPI size check error", major, minor);
        return -1;
    }

    // Mark connection as encrypted and ready
    port->gss->enc = true;
    return 0;
}
```

Key simplifications made:
- Consolidated variable declarations and initialization
- Simplified the main authentication loop structure
- Removed detailed comments in favor of high-level section comments
- Abstracted buffer management details while preserving the core logic
- Streamlined error handling while keeping essential checks
- Focused on the main execution path and protocol flow
- Maintained the two-phase buffer allocation strategy (auth vs normal operation)