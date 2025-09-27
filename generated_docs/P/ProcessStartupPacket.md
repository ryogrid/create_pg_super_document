# ProcessStartupPacket

## Location
[src/backend/tcop/backend_startup.c:453-854](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/backend_startup.c#L453-L854)

## Overview
ProcessStartupPacket reads and processes a client's startup packet, handling protocol negotiation, connection parameters, and special request types like SSL/GSSAPI negotiation or cancel requests.

## Definition

```c
structure.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
```
## Detailed Description
ProcessStartupPacket handles the complex process of reading and interpreting PostgreSQL startup packets from clients. It supports multiple packet types including regular startup packets (protocol version 3), SSL/GSSAPI negotiation requests, and cancel requests. The function validates protocol versions, extracts connection parameters (database name, user name, options), handles encryption layer negotiation, and ensures proper packet formatting. It implements security measures against man-in-the-middle attacks and supports both streaming and logical replication connections.

## Parameters / Member Variables
- : Port structure representing the client connection
- : Flag indicating whether SSL negotiation has been completed
- : Flag indicating whether GSSAPI negotiation has been completed

## Dependencies
- Functions called/Symbols referenced:
  - [pq_startmsgread](../p/pq_startmsgread.md)
  - [pq_getbytes](../p/pq_getbytes.md)
  - [pq_endmsgread](../p/pq_endmsgread.md)
  - pg_ntoh32
  - [processCancelRequest](../p/processCancelRequest.md)
  - [secure_write](../s/secure_write.md)
  - [secure_open_server](../s/secure_open_server.md) (SSL builds)
  - [secure_open_gssapi](../s/secure_open_gssapi.md) (GSSAPI builds)
  - [pq_buffer_remaining_data](../p/pq_buffer_remaining_data.md)
  - [parse_bool](../p/parse_bool.md)
  - [pg_clean_ascii](../p/pg_clean_ascii.md)
  - [SendNegotiateProtocolVersion](../S/SendNegotiateProtocolVersion.md)
  - [pstrdup](../p/pstrdup.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [BackendInitialize](../B/BackendInitialize.md)
  - [ProcessStartupPacket](ProcessStartupPacket.md) (recursive for SSL/GSSAPI negotiation)

## Notes and Other Information
- Returns STATUS_OK for successful processing, STATUS_ERROR for failures or special cases
- Supports protocol versions from PG_PROTOCOL_EARLIEST to PG_PROTOCOL_LATEST
- Handles three special packet types: CANCEL_REQUEST_CODE, NEGOTIATE_SSL_CODE, NEGOTIATE_GSS_CODE
- Validates packet length (4 bytes to MAX_STARTUP_PACKET_LENGTH)
- Extracts standard parameters: database, user, options, replication, application_name
- Truncates database and user names to NAMEDATALEN if too long
- Sets backend type to B_WAL_SENDER for replication connections, B_BACKEND otherwise  
- Implements security check for unencrypted data after encryption negotiation
- Supports protocol options beginning with "_pq_." prefix for future extensions
- Database name defaults to user name if not specified
- Located in src/backend/tcop/backend_startup.c:453-854

## Simplified Source

```c
// Simplified version of ProcessStartupPacket
static int ProcessStartupPacket(Port *port, bool ssl_done, bool gss_done)
{
    int32 len;
    char *buf;
    ProtocolVersion proto;
    MemoryContext oldcontext;

    // Phase 1: Read packet length (4 bytes)
    pq_startmsgread();
    if (pq_getbytes((char *) &len, 1) == EOF) {
        return STATUS_ERROR;  // No data - client disconnected
    }
    if (pq_getbytes(((char *) &len) + 1, 3) == EOF) {
        // Got partial length - report error if not during encryption negotiation
        if (!ssl_done && !gss_done)
            ereport(COMMERROR, (errmsg("incomplete startup packet")));
        return STATUS_ERROR;
    }

    // Phase 2: Validate packet length
    len = pg_ntoh32(len) - 4;  // Convert network byte order, subtract length field
    if (len < sizeof(ProtocolVersion) || len > MAX_STARTUP_PACKET_LENGTH) {
        ereport(COMMERROR, (errmsg("invalid length of startup packet")));
        return STATUS_ERROR;
    }

    // Phase 3: Read packet data
    buf = palloc(len + 1);  // Allocate buffer with null terminator
    buf[len] = '\0';
    if (pq_getbytes(buf, len) == EOF) {
        ereport(COMMERROR, (errmsg("incomplete startup packet")));
        return STATUS_ERROR;
    }
    pq_endmsgread();

    // Phase 4: Process packet based on protocol/request code
    port->proto = proto = pg_ntoh32(*((ProtocolVersion *) buf));

    // Handle cancel request
    if (proto == CANCEL_REQUEST_CODE) {
        CancelRequestPacket *canc = (CancelRequestPacket *) buf;
        if (len != sizeof(CancelRequestPacket)) {
            ereport(COMMERROR, (errmsg("invalid length of startup packet")));
            return STATUS_ERROR;
        }
        int backendPID = pg_ntoh32(canc->backendPID);
        int32 cancelAuthCode = pg_ntoh32(canc->cancelAuthCode);
        processCancelRequest(backendPID, cancelAuthCode);
        return STATUS_ERROR;  // Don't continue processing
    }

    // Handle SSL negotiation request
    if (proto == NEGOTIATE_SSL_CODE && !ssl_done) {
        char SSLok = determine_ssl_support(port);  // Simplified SSL logic

        // Send SSL response to client
        while (secure_write(port, &SSLok, 1) != 1) {
            if (errno == EINTR) continue;
            ereport(COMMERROR, (errmsg("failed to send SSL negotiation response")));
            return STATUS_ERROR;
        }

        // Establish SSL connection if accepted
        if (SSLok == 'S' && secure_open_server(port) == -1)
            return STATUS_ERROR;

        // Security check: no unencrypted data should remain after SSL handshake
        if (pq_buffer_remaining_data() > 0)
            ereport(FATAL, (errmsg("received unencrypted data after SSL request")));

        // Process next packet with SSL established
        return ProcessStartupPacket(port, true, SSLok == 'S');
    }

    // Handle GSSAPI negotiation request (similar to SSL)
    if (proto == NEGOTIATE_GSS_CODE && !gss_done) {
        char GSSok = determine_gss_support(port);  // Simplified GSS logic

        // Send GSS response and establish connection if accepted
        secure_write(port, &GSSok, 1);
        if (GSSok == 'G' && secure_open_gssapi(port) == -1)
            return STATUS_ERROR;

        // Security check for unencrypted data
        if (pq_buffer_remaining_data() > 0)
            ereport(FATAL, (errmsg("received unencrypted data after GSSAPI encryption request")));

        return ProcessStartupPacket(port, GSSok == 'G', true);
    }

    // Phase 5: Process regular startup packet
    FrontendProtocol = proto;

    // Validate protocol version
    if (PG_PROTOCOL_MAJOR(proto) < PG_PROTOCOL_MAJOR(PG_PROTOCOL_EARLIEST) ||
        PG_PROTOCOL_MAJOR(proto) > PG_PROTOCOL_MAJOR(PG_PROTOCOL_LATEST)) {
        ereport(FATAL, (errmsg("unsupported frontend protocol %u.%u",
                               PG_PROTOCOL_MAJOR(proto), PG_PROTOCOL_MINOR(proto))));
    }

    // Phase 6: Extract connection parameters
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    port->guc_options = NIL;

    // Parse name/value pairs from packet
    int32 offset = sizeof(ProtocolVersion);
    while (offset < len) {
        char *name = buf + offset;
        if (*name == '\0') break;  // End of parameters

        char *value = buf + offset + strlen(name) + 1;
        if (offset + strlen(name) + 1 >= len) break;  // Malformed packet

        // Store standard connection parameters
        if (strcmp(name, "database") == 0)
            port->database_name = pstrdup(value);
        else if (strcmp(name, "user") == 0)
            port->user_name = pstrdup(value);
        else if (strcmp(name, "options") == 0)
            port->cmdline_options = pstrdup(value);
        else if (strcmp(name, "replication") == 0)
            handle_replication_parameter(value);  // Sets am_walsender flags
        else if (strcmp(name, "application_name") == 0)
            port->application_name = pg_clean_ascii(value, 0);
        else {
            // Store as GUC option
            port->guc_options = lappend(port->guc_options, pstrdup(name));
            port->guc_options = lappend(port->guc_options, pstrdup(value));
        }

        offset += strlen(name) + 1 + strlen(value) + 1;
    }

    // Phase 7: Finalize connection setup
    validate_connection_parameters(port);  // Check user/database names
    set_backend_type();  // Set MyBackendType based on connection type

    MemoryContextSwitchTo(oldcontext);
    return STATUS_OK;
}
```

Key simplifications made:
- Condensed packet reading into clear phases with descriptive comments
- Abstracted SSL/GSSAPI support determination into helper functions
- Simplified error handling by removing detailed conditional checks
- Consolidated parameter parsing loop with clearer logic flow
- Abstracted replication parameter handling and connection validation
- Removed platform-specific #ifdef blocks for clarity
- Focused on the main execution path while preserving essential security checks
- Consolidated similar SSL and GSSAPI negotiation patterns